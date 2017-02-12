# frozen_string_literal: true
module Dataflow
  module Nodes
    module Filter
      # Select records that match the condition.
      class WhereNode < ComputeNode
        VALID_OPS = %w(eq ne le lt ge gt).freeze

        field :key,     type: String, required_for_computing: true
        field :op,      type: String, required_for_computing: true, values: VALID_OPS
        field :value,                 required_for_computing: true

        ensure_data_node_exists
        ensure_dependencies exactly: 1

        private

        def compute_batch(records:)
          where(records: records)
        end

        def where(records:)
          tokens = record_dig_tokens(key: key, use_sym: dependencies.first.use_symbols?)
          case op.to_s.downcase
          when 'eq'
            records.select { |x| x.dig(*tokens) == value }
          when 'ne'
            records.select { |x| x.dig(*tokens) != value }
          when 'le'
            records.select { |x| x.dig(*tokens) <= value }
          when 'lt'
            records.select { |x| x.dig(*tokens) < value }
          when 'ge'
            records.select { |x| x.dig(*tokens) >= value }
          when 'gt'
            records.select { |x| x.dig(*tokens) > value }
          else
            raise Errors::InvalidConfigurationError, "Invalid op key: #{op}"
          end
        end
      end
    end
  end
end
