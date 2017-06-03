# frozen_string_literal: true
module Dataflow
  module Nodes
    # Data nodes are used to build a data computing/transformation graph.
    # At each step we can save the results to a (temp) table.
    #
    # Nodes::DataNode represents one of the data nodes.
    # It is meant to be treated as an interface and should not be used directly.
    class DataNode
      include Mongoid::Document
      include Dataflow::Node
      include Dataflow::PropertiesMixin
      include Dataflow::EventMixin
      include Dataflow::SchemaMixin

      event :schema_inference_started
      event :schema_inference_progressed
      event :schema_inference_finished

      event :export_started
      event :export_progressed
      event :export_finished

      # make sure we have only one node per db/table combination
      index({ db_name: 1, name: 1 }, unique: true)

      # The dataset name used by this node for storage.
      field :name, type: String, editable: false

      # The database name used by this node
      field :db_name, type: String, editable: false
      # The database host (used the ENV settings by default)
      field :db_host, type: String, editable: false
      # The database port (used the ENV settings by default)
      field :db_port, type: String, editable: false
      # The database user (used the ENV settings by default)
      field :db_user, type: String, editable: false
      # The database password (used the ENV settings by default)
      field :db_password, type: String, editable: false

      # The schema of this node
      field :schema,                  type: Hash,    editable: false
      field :inferred_schema,         type: Hash,    editable: false
      field :inferred_schema_at,      type: Time,    editable: false
      # How many samples were used to infer the schema
      field :inferred_schema_from,    type: Integer, editable: false

      # The time when this node was last updated
      field :updated_at,              type: Time, editable: false

      # One of the possible backend this node will use e.g.: :mongodb, :csv, :mysql
      field :db_backend,              type: Symbol, editable: false, default: :mongodb

      # Represents the time in seconds within which to expect an update on this node
      field :update_expected_within,  type: Integer,                 default: 0

      # The indexes this node will implement on its dataset.
      # Indexes should be in the following format:
      # [
      #   { key: 'id' },
      #   { key: 'updated_at' },
      #   { key: ['id', 'updated_at'], unique: true }
      # ]
      field :indexes,              type: Array, default: []

      # whether to use double buffering or not
      field :use_double_buffering, type: Boolean,   editable: false, default: false

      # internal use: where to read/write from. Use 1 and 2 for legacy reasons.
      field :read_dataset_idx,     type: Integer,   editable: false, default: 1
      field :write_dataset_idx,    type: Integer,   editable: false, default: 2

      # Necessary fields:
      validates_presence_of :db_name
      validates_presence_of :name

      # Before create: run default initializations
      before_create :set_defaults

      # Sets the default parameters before creating the object.
      def set_defaults
        self.schema = schema || {}

        # Use the schema as the inferred schema if none is provided.
        # This useful when there is no need to infer schemas (e.g. in SQL)
        self.inferred_schema ||= schema

        # This is needed for the flow to compute properly
        self.updated_at = Time.now
      end

      # Callback: after creation make sure the underlying dataset matches this node's properties.
      after_create do
        handle_dataset_settings_changed
      end

      # Callback: after save, make sure the underlying dataset is valid if
      # any dataset-related proprety changed.
      after_save do
        if name_changed? || indexes_changed? || db_backend_changed?
          handle_dataset_settings_changed
        end
      end

      # When the dataset properties changed notify the adapter to handle the new settings.
      def handle_dataset_settings_changed
        db_adapter.update_settings(data_node: self)

        # if we're using double buffering, just wait for the next buffer
        # to be created to apply the changes.
        return if use_double_buffering

        # recreate the dataset if there is no data
        if db_adapter.count.zero?
          db_adapter.recreate_dataset(dataset: read_dataset_name)
        end

        db_adapter.create_indexes(dataset: read_dataset_name)
      end

      # Finds and return from the dataset, based on the given options.
      # @param where [Hash] the condition to apply for retrieving the element.
      #             e.g.: { 'id' => 1 } will fetch a record with the id 1.
      #             An empty option hash will retrieve any record.
      # @return [Hash] returns a single record from the dataset.
      def find(where: {})
        db_adapter.find(where: where)
      end

      # Returns all the records from a dataset that match the options.
      # @param where [Hash] the condition to apply for retrieving the element.
      #             e.g.: { 'id' => 1 } will fetch a record with the id 1.
      #             An empty option hash will retrieve any record.
      # @param fields [Array] Array of strings representing which fields to include.
      #               e.g.: ['id', 'updated_at'] will only return these two fields.
      # @param sort [Hash] represents the sorting of the returned dataset.
      #             e.g. { 'id' => 1, 'updated_at' => -1 } will sort by
      #             id ASC and by updated_at DESC.
      # @param limit [Integer] limits the amount of records returned.
      # @param offset [Integer] starting offset of the records returned.
      #        Use with limit to implement pagination.
      # @yield [db_client] When a block is passed, yields the db client on which .each
      #        can be called to stream the results rather than load everything in memory.
      #        Other methods can also be called depending on the backend,
      #        the downside being back-end portability (use at your own risk).
      def all(where: {}, fields: [], sort: {}, limit: 0, offset: 0, &block)
        db_adapter.all(where: where, fields: fields, sort: sort, limit: limit, offset: offset, &block)
      end

      # Supports paginating efficiently through the dataset.
      # @param where [Hash] the condition to apply for retrieving the element.
      #             e.g.: { 'id' => 1 } will fetch a record with the id 1.
      #             An empty option hash will retrieve any record.
      #             IMPORTANT: do not use the system id in the query. It will be overwritten.
      # @param fields [Array] Array of strings representing which fields to include.
      #               e.g.: ['id', 'updated_at'] will only return these two fields.
      # @param limit [Integer] limits the amount of records returned.
      # @param cursor [String] indicates from which page should the results be returned.
      # @return [Hash] with 2 fields:
      #         - data [Array] that contains the fetched records
      #         - next_cursor [String] a string to pass into the sub-sequent
      #                                calls to fetch the next page of the data
      def all_paginated(where: {}, fields: [], cursor: nil)
        db_adapter.all_paginated(where: where, fields: fields, cursor: cursor)
      end

      # Return a list of order (ASC) system IDs.
      # @param batch_size [Integer] how many IDs to select per query.
      # These can be used to process the dataset in parallel by querying on a sub-section:
      # queries = node.ordered_system_id_queries
      # Parallel.each(queries) do |query|
      #   process(node.all(where: query))
      # end
      def ordered_system_id_queries(batch_size:, where: {})
        db_adapter.ordered_system_id_queries(batch_size: batch_size, where: {})
      end

      # Counts how many records matches the condition or all if no condition is given.
      # @return [Integer] the record count.
      def count(where: {})
        db_adapter.count(where: where)
      end

      # Adds the given records to the dataset and updates the updated_at time.
      # @param records [Array] an array of the records to be added.
      def add(records:)
        raise ArgumentError, "records must be an array of documents. Received: '#{records.class}'." unless records.is_a?(Array)
        records = records.compact
        return if records.blank?
        db_adapter.save(records: records)
        self.updated_at = Time.now
        save!
      end

      # Clear the data that matches the options.
      def clear(where: {})
        db_adapter.delete(where: where)
      end

      # Update this node's schema.
      def update_schema(sch)
        self.schema = sch
        db_adapter.update_settings(data_node: self)
      end

      # Recreates a dataset.
      # @param dataset_type [Symbol] select which dataset to recreate.
      #        Can :read or :write.
      def recreate_dataset(dataset_type: :read)
        # fetch the proper dataset name
        dataset = send("#{dataset_type}_dataset_name")
        db_adapter.recreate_dataset(dataset: dataset)
      end

      # Applies unique indexes on the dataset.
      # As this will be enforcing constraints, it is best applied
      # before adding any data.
      # @param dataset_type [Symbol] select which dataset to recreate.
      #        Can :read or :write.
      def create_unique_indexes(dataset_type: :read)
        dataset = send("#{dataset_type}_dataset_name")
        db_adapter.create_indexes(dataset: dataset, type: :unique_only)
      end

      # Applies non-unique indexes on the dataset.
      # For performance reasons, these indexes are best applied
      # after adding data (especially on large import operations).
      def create_non_unique_indexes(dataset_type: :read)
        dataset = send("#{dataset_type}_dataset_name")
        db_adapter.create_indexes(dataset: dataset, type: :non_unique_only)
      end

      def read_dataset_name
        return @temporary_read_dataset if @temporary_read_dataset

        if use_double_buffering
          "#{name}_buffer#{read_dataset_idx}"
        else
          name
        end
      end

      def write_dataset_name
        if use_double_buffering
          "#{name}_buffer#{write_dataset_idx}"
        else
          name
        end
      end

      # Use to select from which dataset you want to read.
      # A possible use case is to read from an old dataset name.
      # @param dataset [String] the dataset name from where to read from.
      #        It must be a valid dataset name for the current settings.
      def read_dataset_name=(dataset)
        return unless valid_dataset_names.include?(dataset)
        @temporary_read_dataset = dataset
        db_adapter.update_settings(data_node: self)
        dataset
      end

      def swap_read_write_datasets!
        raise Dataflow::Errors::InvalidConfigurationError, '#swap_read_write_dataset_names! called on "#{self.name}" but "use_double_buffering" is not activated.' unless use_double_buffering
        tmp = read_dataset_idx
        self.read_dataset_idx = write_dataset_idx
        self.write_dataset_idx = tmp
        db_adapter.update_settings(data_node: self)
        save!
      end

      def import(connection_opts: {}, keys: nil)
        importer = db_adapter(connection_opts)
        records = importer.all
        add(records: records)
      end

      def export(connection_opts: { db_backend: :csv }, keys: [], where: {})
        on_export_started(connection_opts: connection_opts, keys: keys)
        # instanciate and export without saving anything
        Export::ToCsvNode.new(
          dependency_ids: [self],
          query: where.to_json,
          keys: keys
        ).compute_impl
        on_export_finished
      end

      # retrieves some informations about this node and its usage
      def info(write_dataset: false)
        dataset = write_dataset ? write_dataset_name : read_dataset_name
        usage = db_adapter.usage(dataset: dataset)
        {
          name: name,
          type: self.class.to_s,
          dataset: dataset,
          db_backend: db_backend,
          updated_at: updated_at,
          record_count: count,
          indexes: indexes,
          effective_indexes: usage[:effective_indexes],
          mem_usage: usage[:memory],
          storage_usage: usage[:storage]
        }
      end

      def use_symbols?
        (db_backend.to_s =~ /sql/).present?
      end

      def updated?
        true
      end

      def explain_update(depth: 0, verbose: false)
        logger.log("#{'>' * (depth + 1)} #{name} [Dataset] | UPDATED = #{updated_at}")
      end

      def required_by
        super + Dataflow::Nodes::ComputeNode.where(data_node_id: _id).map { |node|
          { node: node, type: 'dataset' }
        }
      end

      # this is not safe if there is some parallel processing going on
      def safely_clear_write_dataset
        # we can only clear the write dataset if we're using double buffering
        return unless use_double_buffering
        # check if there is any node that is currently computing to this dataset
        used_by = required_by.select { |x| x[:type] == 'dataset' && x[:node].locked_for_computing? }
        return if used_by.present?

        logger.log("Dropping #{db_name}.#{write_dataset_name} on #{db_backend}.")
        # TODO: lock the node?
        db_adapter.drop_dataset(write_dataset_name)
      end

      def drop_dataset!
        db_adapter.drop_dataset(write_dataset_name)
        return unless use_double_buffering
        db_adapter.drop_dataset(read_dataset_name)
      end

      # Dump a backup of this dataset to a file.
      # @return [String] the filepath to the dump file.
      def dump_dataset(base_folder: './dump')
        db_adapter.dump(base_folder: base_folder)
      end

      # Restore a dump of this dataset
      # @param files [String] the filepath to the dump file.
      def restore_dataset(filepath:)
        db_adapter.restore(filepath: filepath)
      end

      private

      def db_adapter(connection_opts = {})
        db_backend = connection_opts[:db_backend] || self.db_backend

        opts = connection_opts.deep_dup
        opts.delete(:db_backend)
        has_options = opts.present?

        case db_backend.downcase.to_s
        when 'mongodb'
          return Adapters::MongoDbAdapter.new(opts) if has_options
          @mongodb_adapter ||= Adapters::MongoDbAdapter.new(data_node: self)
          return @mongodb_adapter
        when 'csv'
          return Adapters::CsvAdapter.new(opts) if has_options
          @csv_adapter ||= Adapters::CsvAdapter.new(data_node: self)
          return @csv_adapter
        when 'mysql'
          opts[:adapter_type] = 'mysql'
          return Adapters::SqlAdapter.new(opts) if has_options
          @mysql_adapter ||= Adapters::MysqlAdapter.new(data_node: self, adapter_type: 'mysql')
          return @mysql_adapter
        when 'postgresql'
          opts[:adapter_type] = 'postgresql'
          return Adapters::SqlAdapter.new(opts) if has_options
          @postgresql_adapter ||= Adapters::PsqlAdapter.new(data_node: self, adapter_type: 'postgresql')
          return @postgresql_adapter
        end

        raise NotImplementedError, "'#{db_backend}' backend is not implemented."
      end

      def valid_dataset_names
        if use_double_buffering
          ["#{name}_buffer1", "#{name}_buffer2"]
        else
          [name]
        end
      end

      def logger
        @logger ||= Dataflow::Logger.new(prefix: 'Dataflow')
      end
    end # class DataNode
  end # module Nodes
end # module Dataflow
