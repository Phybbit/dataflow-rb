# frozen_string_literal: true
module Dataflow
  module Nodes
    # Transforms the dependency's dataset to a SQL-compatible one.
    class SqlQueryNode < ComputeNode
      ensure_data_node_exists
      ensure_dependencies min: 0 # dependencies are not necessarily needed
      field :query, type: String, required_for_computing: true

      def valid_for_computation?
        unless (data_node&.db_backend.to_s =~ /sql/).present?
          errors.add(:db_backend, 'Must have a SQL based backend.')
        end

        begin
          computed_query
        rescue StandardError => e
          errors.add(:query, "Specified query has errors: #{e.message}")
        end

        super
      end

      def computed_query
        # 1. replace the current write dataset's name
        q = query.gsub('<node>', write_dataset_name)

        # 2. replace the dependencies' (read) dataset names
        q.gsub(/<[0-9]+>/) do |match|
          # [1..-2] will remove the 'less than' < and 'greater than' >
          dep_index = match[1..-2].to_i
          raise "Specified depependency #{match} does not exist. There are only #{dependencies.count} dependencies." if dep_index >= dependencies.count
          dependencies[dep_index].read_dataset_name
        end
      end

      def execute_query
        query = computed_query
        logger.log(query)
        data_node.send(:db_adapter).client[query].to_a
      end

      private

      # Overrides the base implementation.
      # This node will leave all the work to the DB.
      def compute_impl
        execute_query
      end
    end
  end
end
