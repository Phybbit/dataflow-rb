# frozen_string_literal: true
module Dataflow
  module Nodes
    # Represents a compution. May stores its output in a separate data node.
    # It depends on other data nodes to compute its own data.
    class ComputeNode
      include Mongoid::Document
      include Dataflow::Node
      include Dataflow::PropertiesMixin
      include Dataflow::EventMixin
      include Dataflow::SchemaMixin

      event :computing_started    # handler(node)
      event :computing_progressed # handler(node, pct_complete)
      event :computing_finished   # handler(node, state)

      delegate :find, :all, :all_paginated, :count, :ordered_system_id_queries,
               :db_backend, :db_name, :use_symbols?,
               :schema, :read_dataset_name, :write_dataset_name,
               to: :data_node

      #############################################
      # Dependencies definition
      #############################################
      class << self
        def dependency_opts
          @dependency_opts || {}
        end

        def data_node_opts
          @data_node_opts || {}
        end

        # DSL to be used while making computeqd nodes. It supports enforcing validations
        # by checking whether there is exactly, at_least (min) or at_most (max)
        # a given number of dependencies. Usage:
        # class MyComputeNode < ComputeNode
        #   ensure_dependencies exactly: 1 # could be e.g.: min: 3, or max: 5
        # end
        def ensure_dependencies(opts)
          raise Dataflow::Errors::InvalidConfigurationError, "ensure_dependencies must be given a hash. Received: #{opts.class}" unless opts.is_a?(Hash)
          valid_keys = %i(exactly min max).freeze
          has_attributes = (valid_keys - opts.keys).count < valid_keys.count
          raise Dataflow::Errors::InvalidConfigurationError, "ensure_dependencies must have at least one of 'min', 'max' or 'exactly' attributes set. Given: #{opts.keys}" unless has_attributes

          add_property(:dependency_ids, opts)
          @dependency_opts = opts
        end

        # DSL to ensure that a data node must be set before a computed node
        # can be recomputed (as it will presumably use it to store data).
        def ensure_data_node_exists
          @data_node_opts = { ensure_exists: true }
        end
      end

      # The node name
      field :name,                        type: String

      # The data node to which we will write the computation output
      field :data_node_id,                type: BSON::ObjectId

      # Whether to clear the data from the data node before computing
      field :clear_data_on_compute,       type: Boolean, default: true

      # The dependencies this node requires for computing.
      field :dependency_ids,              type: Array, default: []

      # Represents the maximum record count that should be used
      # per process during computation.
      field :limit_per_process,           type: Integer, default: 0

      # Use automatic recomputing interval. In seconds.
      field :recompute_interval,          type: Integer, default: 0

      # Used as a computing lock. Will be set to 'computing'
      # if currently computing or nil otherwise.
      field :computing_state,             type: String,   editable: false

      # When has the computing started.
      field :computing_started_at,        type: Time,     editable: false

      # Indicates the last time a successful computation has started.
      field :last_compute_starting_time,  type: Time,     editable: false

      # The last time an heartbeat was received.
      # Useful to detect stale computation that need to be reaped.
      field :last_heartbeat_time,         type: Time,     editable: false

      # Necessary fields:
      validates_presence_of :name

      # Before create: run default initializations
      before_create :set_defaults

      # Sets the default parameters before creating the object.
      def set_defaults
        # support setting the fields with a Document rather
        # than an ObjectId. Handle the transformations here:
        if data_node_id.present?
          self.data_node_id = data_node_id._id unless data_node_id.is_a?(BSON::ObjectId)

          # the data node use_double_buffering setting
          # must match clear_data_on_compute:
          if data_node.use_double_buffering != clear_data_on_compute
            data_node.use_double_buffering = clear_data_on_compute
            data_node.save
          end
        end

        # Again support having an ObjectId or a document.
        self.dependency_ids = dependency_ids.map { |dep|
          next dep if dep.is_a? BSON::ObjectId
          dep._id
        }

        # Update the data node schema with the required schema
        # for this computed node.
        data_node&.update_schema(required_schema)
      end

      # Fetch the data node if it is set
      def data_node
        @data_node ||= Dataflow::Nodes::DataNode.find(data_node_id) if data_node_id.present?
      end

      # Override the relation because self.dependencies is not ordered.
      def dependencies(reload: false)
        return @dependencies if @dependencies.present? && !reload
        @dependencies = dependency_ids.map do |x|
          Dataflow::Node.find(x)
        end
      end

      # retrieve the whole dependency tree
      def all_dependencies
        (dependencies + dependencies.flat_map(&:all_dependencies)).uniq
      end

      # Returns false if any of our dependencies has
      # been updated after our last update.
      # We define a computed node's last update as the time it started its
      # last successful update (instead of the time it completed it, has
      # dependencies may have changed in the mean time).
      # @return [Boolean]
      def updated?
        return false if updated_at.blank?

        dependencies.each do |dependency|
          return false unless dependency.updated?
          return false if dependency.updated_at > updated_at
        end
        true
      end

      # Keep a uniform interface with a DataNode.
      def updated_at
        last_compute_starting_time
      end

      def updated_at=(val)
        self.last_compute_starting_time = val
      end

      # Checks whether an automatic recomputing is needed.
      # @return [Boolean]
      def needs_automatic_recomputing?
        interval = recompute_interval.to_i
        return false if interval <= 0
        return false if updated?
        return false if locked_for_computing?
        return true if updated_at.blank?

        updated_at + interval.seconds < Time.now
      end

      # Update the dependencies that need to be updated
      # and then compute its own data.
      # @param force_recompute [Boolean] if true, computes
      #        even if the node is already up to date.
      def recompute(depth: 0, force_recompute: false)
        logger.log "#{'>' * (depth + 1)} #{name} started recomputing..."
        start_time = Time.now

        parallel_each(dependencies) do |dependency|
          logger.log "#{'>' * (depth + 1)} #{name} checking deps: #{dependency.name}..."
          if !dependency.updated? || force_recompute
            dependency.recompute(depth: depth + 1, force_recompute: force_recompute)
          end
        end

        # Dependencies data may have changed in a child process.
        # Reload to make sure we have the latest metadata.
        logger.log "#{'>' * (depth + 1)} #{name} reloading dependencies..."
        dependencies(reload: true)

        compute(depth: depth, force_compute: force_recompute)
        logger.log "#{'>' * (depth + 1)} #{name} took #{Time.now - start_time} seconds to recompute."

        true
      end

      # Compute this node's data if not already updated.
      # Acquires a computing lock before computing.
      # In the eventuality that the lock is already acquired, it awaits
      # until it finishes or times out.
      # @param force_compute [Boolean] if true, computes
      #        even if the node is already up to date.
      def compute(depth: 0, force_compute: false, source: nil)
        has_compute_lock = false
        validate!

        if updated? && !force_compute
          logger.log "#{'>' * (depth + 1)} #{name} is up-to-date."
          return
        end

        has_compute_lock = acquire_computing_lock!
        if has_compute_lock
          logger.log "#{'>' * (depth + 1)} #{name} started computing."
          on_computing_started
          start_time = Time.now

          # update this node's schema with the necessary fields
          data_node&.update_schema(required_schema)

          pre_compute(force_compute: force_compute)

          if clear_data_on_compute
            # Pre-compute, we recreate the table, the unique indexes
            data_node&.recreate_dataset(dataset_type: :write)
            data_node&.create_unique_indexes(dataset_type: :write)
          end

          send_heartbeat
          compute_impl

          if clear_data_on_compute
            # Post-compute, delay creating other indexes for insert speed
            data_node&.create_non_unique_indexes(dataset_type: :write)
            # swap read/write datasets
            data_node&.swap_read_write_datasets!
          end

          self.last_compute_starting_time = start_time
          duration = Time.now - start_time
          logger.log "#{'>' * (depth + 1)} #{name} took #{duration} seconds to compute."
          on_computing_finished(state: 'computed')
        else
          logger.log "#{'>' * (depth + 1)} [IS AWAITING] #{name}."
          await_computing!
          logger.log "#{'>' * (depth + 1)} [IS DONE AWAITING] #{name}."
        end

      rescue StandardError => e
        on_computing_finished(state: 'error', error: e) if has_compute_lock
        logger.log "#{'>' * (depth + 1)} [ERROR] #{name} failed computing: #{e}"
        raise
      ensure
        release_computing_lock! if has_compute_lock
        true
      end

      # Check wethere this node can or not compute.
      # Errors are added to the active model errors.
      # @return [Boolean] true has no errors and can be computed.
      def valid_for_computation?
        # Perform additional checks: also add errors to "self.errors"
        opts = self.class.dependency_opts
        if opts.key?(:exactly)
          ensure_exact_dependencies(count: opts[:exactly])
        elsif opts.key?(:max)
          ensure_at_most_dependencies(count: opts[:max])
        else # even if the min is not specified, we need at least 1 dependency
          ensure_at_least_dependencies(count: opts[:min] || 1)
        end
        ensure_no_cyclic_dependencies
        ensure_keys_are_set
        ensure_data_node_exists if self.class.data_node_opts[:ensure_exists]

        errors.count == 0
      end

      # Check this node's locking status.
      # @return [Boolean] Whtere this node is locked or not.
      def locked_for_computing?
        computing_state == 'computing'
      end

      # Force the release of this node's computing lock.
      # Do not use unless there is a problem with the lock.
      def force_computing_lock_release!
        release_computing_lock!
      end

      private

      # Compute implementation:
      # - recreate the table
      # - compute the records
      # - save them to the DB
      # (the process may be overwritten on a per-node basis if needed)
      def compute_impl
        process_parallel(node: dependencies.first)
      end

      def process_parallel(node:)
        record_count = node.count
        return if record_count == 0

        equal_split_per_process = (record_count / Parallel.processor_count.to_f).ceil
        count_per_process = equal_split_per_process
        limit = limit_per_process.to_i
        count_per_process = [limit, equal_split_per_process].min if limit > 0

        queries = node.ordered_system_id_queries(batch_size: count_per_process)

        parallel_each(queries.each_with_index) do |query, idx|
          send_heartbeat
          progress = (idx / queries.count.to_f * 100).ceil
          on_computing_progressed(pct_complete: progress)

          records = node.all(where: query)

          new_records = if block_given?
                          yield records
                        else
                          compute_batch(records: records)
                        end

          data_node.add(records: new_records)
        end
      end

      # This is an interface only.
      # Override with record computation logic.
      def compute_batch(records:)
        records
      end

      def acquire_computing_lock!
        # make sure that any pending changes are saved.
        save
        find_query = { _id: _id, computing_state: { '$ne' => 'computing' } }
        update_query = { '$set' => { computing_state: 'computing', computing_started_at: Time.now } }
        # send a query directly to avoid mongoid's caching layers
        res = Dataflow::Nodes::ComputeNode.where(find_query).find_one_and_update(update_query)
        # reload the model data after the query above
        reload
        # the query is atomic so if res != nil, we acquired the lock
        !res.nil?
      end

      def release_computing_lock!
        # make sure that any pending changes are saved.
        save
        find_query = { _id: _id }
        update_query = { '$set' => { computing_state: nil, computing_started_at: nil } }
        # send a query directly to avoid mongoid's caching layers
        Dataflow::Nodes::ComputeNode.where(find_query).find_one_and_update(update_query)
        # reload the model data after the query above
        reload
      end

      def await_computing!
        start_waiting_at = Time.now
        # TODO: should the max wait time be dependent on e.g. the recompute interval?
        max_wait_time = 15.minutes
        while Time.now < start_waiting_at + max_wait_time
          sleep 2
          # reloads with the data stored on mongodb:
          # something maybe have been changed by another process.
          reload
          return unless locked_for_computing?
        end

        raise StandardError, "Awaiting computing on #{name} reached timeout."
      end

      # Interface only. Re-implement for node-specific behavior before computing
      def pre_compute(force_compute:); end

      # Override to define a required schema.
      def required_schema
        schema
      end

      def send_heartbeat
        update_query = { '$set' => { last_heartbeat_time: Time.now } }
        Dataflow::Nodes::ComputeNode.where(_id: _id)
                                    .find_one_and_update(update_query)
      end

      ##############################
      # Dependency validations
      ##############################

      def ensure_no_cyclic_dependencies
        node_map = Dataflow::Nodes::ComputeNode.all.map { |n| [n._id, n] }.to_h

        dep_ids = (dependency_ids || [])
        dep_ids.each do |dependency_id|
          next unless has_dependency_in_hierarchy?(node_map[dependency_id], dependency_id, node_map)
          error_msg = "Dependency to node #{dependency_id} ('#{node_map[dependency_id].name}') is cylic."
          errors.add(:dependency_ids, error_msg)
        end
      end

      def has_dependency_in_hierarchy?(node, dependency_id, node_map)
        return false if node.blank?
        # if we're reach a node that has no more deps, then we did not find
        # the given dependency_id in the hierarchy
        return true if (node.dependency_ids || []).include?(dependency_id)
        (node.dependency_ids || []).any? do |dep_id|
          has_dependency_in_hierarchy?(node_map[dep_id], dependency_id, node_map)
        end
      end

      def ensure_no_cyclic_dependencies!
        ensure_no_cyclic_dependencies
        raise_dependendy_errors_if_needed!
      end

      def ensure_exact_dependencies(count:)
        # we need to use .size, not .count
        # for the mongo relation to work as expected
        current_count = (dependency_ids || []).size
        return if current_count == count

        error_msg = "Expecting exactly #{count} dependencies. Has #{current_count} dependencies."
        errors.add(:dependency_ids, error_msg)
      end

      def ensure_at_least_dependencies(count:)
        # we need to use .size, not .count
        # for the mongo relation to work as expected
        current_count = (dependency_ids || []).size
        return if current_count >= count

        error_msg = "Expecting at least #{count} dependencies. Has #{current_count} dependencies."
        errors.add(:dependency_ids, error_msg)
      end

      def ensure_at_most_dependencies(count:)
        # we need to use .size, not .count
        # for the mongo relation to work as expected
        current_count = (dependency_ids || []).size
        return if current_count <= count

        error_msg = "Expecting at most #{count} dependencies. Has #{current_count} dependencies."
        errors.add(:dependency_ids, error_msg)
      end

      def ensure_keys_are_set
        required_keys = self.class.properties.select { |_k, opts| opts[:required_for_computing] }
        required_keys.each do |key, opts|
          errors.add(key, "#{self.class}.#{key} must be set for computing.") if self[key].nil?
          if opts[:values].is_a?(Array)
            # make sure the key's value is one of the possible values
            errors.add(key, "#{self.class}.#{key} must be set to one of #{opts[:values].join(', ')}. Given: #{self[key]}") unless opts[:values].include?(self[key])
          end
        end
      end

      def ensure_data_node_exists
        if data_node_id.blank?
          error_msg = 'Expecting a data node to be set.'
          errors.add(:data_node_id, error_msg)
          return
        end

        # the data node id is present. Check if it found
        Dataflow::Nodes::DataNode.find(data_node.id)
      rescue Mongoid::Errors::DocumentNotFound
        # it was not found:
        error_msg = "No data node was found for Id: '#{data_node_id}'."
        errors.add(:data_node_id, error_msg)
      end

      def parallel_each(itr)
        # before fork: always disconnect currently used connections.
        Dataflow::Adapters::SqlAdapter.disconnect_clients
        Dataflow::Adapters::MongoDbAdapter.disconnect_clients
        Mongoid.disconnect_clients

        # set to true to debug code in the iteration
        is_debugging_impl = (ENV['RACK_ENV'] == 'test' && ENV['DEBUG'])
        if is_debugging_impl # || true
          itr.each do |*args|
            yield(*args)
          end
        else
          Parallel.each(itr) do |*args|
            yield(*args)
            Dataflow::Adapters::SqlAdapter.disconnect_clients
            Dataflow::Adapters::MongoDbAdapter.disconnect_clients
            Mongoid.disconnect_clients
          end
        end
      end

      def logger
        @logger ||= Dataflow::Logger.new(prefix: 'Dataflow')
      end
    end # class ComputeNode
  end # module Nodes
end # module Dataflow
