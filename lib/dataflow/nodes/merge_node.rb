# frozen_string_literal: true
module Dataflow
  module Nodes
    # Performs a merge operation on 2 dependencies.
    class MergeNode < ComputeNode
      field :merge_key,    type: String, default: ''
      field :merge_values, type: Array,  default: []

      ensure_data_node_exists
      ensure_dependencies exactly: 2

      private

      def compute_impl
        process_parallel(node: dependencies.first) do |records|
          merge_records(records: records, index: 0)
        end

        process_parallel(node: dependencies.second) do |records|
          merge_records(records: records, index: 1)
        end
      end

      def merge_records(records:, index:)
        records.each do |record|
          # add a merge key with the corresponding value if necessary
          record[merge_key] = merge_values[index] if merge_key.present?
        end
        records
      end
    end
  end
end
