# frozen_string_literal: true
module Dataflow
  module Nodes
    # Performs a map operation on 2 dependencies.
    class MapNode < ComputeNode
      ensure_data_node_exists
      ensure_dependencies exactly: 2

      private

      def compute_batch(records:)
        map(records: records, mapping_node: dependencies.second)
      end

      def map(records:, mapping_node:)
        mapping_table = mapping_node.all

        records.each do |record|
          mapping_table.each { |mapping| map_record(record, mapping) }
        end

        records
      end

      def map_record(record, mapping)
        original_key = mapping['key']
        original_value = record_value(record: record, key: original_key)
        mapped_key = mapping['mapped_key']
        mapped_value = nil

        if mapping['map'].present?
          # re-map either the key/value with a lambda(key,value)
          result = eval(mapping['map']).call(original_key, original_value)
          mapped_key = result.keys[0]
          mapped_value = result.values[0]
        elsif mapping['values'].is_a? Hash
          # or from a hash-table that directly translates values
          mapped_value = mapping['values'][original_value]
          mapped_value ||= mapping['default']
        elsif mapping['values'].present?
          # or map the current value with a lambda(value)
          mapped_value = eval(mapping['values']).call(original_value)
        end

        mapped_key ||= original_key
        record[mapped_key] = mapped_value || original_value
      end
    end
  end
end
