# frozen_string_literal: true
module Dataflow
  module Nodes
    module Transformation
      # Transforms the given keys' values to Time.
      class ToTimeNode < ComputeNode
        field :keys, type: Array, required_for_computing: true, default: []

        ensure_data_node_exists
        ensure_dependencies exactly: 1

        def valid_for_computation?
          # It does not make sense to use this node without any keys specified.
          if (keys || []).count.zero?
            errors.add(:keys, "#{self.class} keys must contain at least 1 value")
          end

          super
        end

        def compute_batch(records:)
          key_tokens = keys.map do |key|
            record_dig_tokens(key: key, use_sym: dependencies.first.use_symbols?)
          end

          records.each do |record|
            key_tokens.each_with_index do |tokens, index|
              value = record.dig(*tokens)
              next unless value.present?

              value = value.to_time
              add_value_to_record(record: record, key: keys[index], value: value)
            end
          end

          records
        end
      end
    end
  end
end
