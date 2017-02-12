# frozen_string_literal: true
module Dataflow
  module Nodes
    # Performs a select operation on its dependency.
    class SelectKeysNode < ComputeNode
      field :keys,       type: Array, required_for_computing: true

      ensure_data_node_exists
      ensure_dependencies exactly: 1

      def export(connection_opts: { db_backend: :csv }, keys: nil)
        super(connection_opts: connection_opts, keys: keys || self.keys)
      end

      private

      def compute_batch(records:)
        k = keys
        k = k.map(&:to_sym) if dependencies.first.use_symbols?
        select_keys(records: records, keys: k)
      end

      def select_keys(records:, keys:)
        records.map do |base_record|
          new_record = {}
          keys.each do |key|
            value = record_value(record: base_record, key: key)
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
