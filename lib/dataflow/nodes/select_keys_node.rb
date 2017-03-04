# frozen_string_literal: true
module Dataflow
  module Nodes
    # Performs a select operation on its dependency.
    class SelectKeysNode < ComputeNode
      field :keys,       type: Array, required_for_computing: true

      ensure_data_node_exists
      ensure_dependencies exactly: 1

      def export
        data_node.export(keys: keys)
      end

      private

      def compute_batch(records:)
        keys_tokens = keys.map { |k| [k, record_dig_tokens(key: k, use_sym: dependencies.first.use_symbols?)] }
        select_keys(records: records, keys_tokens: keys_tokens)
      end

      def select_keys(records:, keys_tokens:)
        records.map do |base_record|
          new_record = {}
          keys_tokens.each do |key, tokens|
            value = base_record.dig(*tokens)
            next unless value.present?

            add_value_to_record(record: new_record, key: key, value: value)
          end

          next unless new_record.present?
          new_record
        end.compact
      end
    end
  end
end
