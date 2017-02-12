# frozen_string_literal: true
module Dataflow
  module Nodes
    module Filter
      # Makes a sequency based on a key (e.g. id), and order it (e.g. by time),
      # and then applies the same logic as ruby's drop_while.
      # See: https://ruby-doc.org/core-2.4.0/Array.html#method-i-drop_while
      class DropWhileNode < ComputeNode
        VALID_OPS = %w(eq ne le lt ge gt).freeze
        VALID_MODES = %w(both left right).freeze

        # group by the id key
        field :id_key,    type: String,  required_for_computing: true
        # then sort by the sort_by
        field :sort_by,   type: String,  required_for_computing: true
        field :sort_asc,  type: Boolean, required_for_computing: true, default: true

        # the apply a drop_while on { field op value }
        field :field,     type: String,  required_for_computing: true
        field :op,        type: String,  required_for_computing: true, values: VALID_OPS
        field :value,     required_for_computing: true
        field :drop_mode, type: String, required_for_computing: true, values: VALID_MODES, default: VALID_MODES[0]

        ensure_data_node_exists
        ensure_dependencies exactly: 1

        def compute_impl
          base_node = dependencies.first
          records_count = base_node.count
          return if records_count == 0

          ids = base_node.all(fields: [id_key]) do |results|
            results.distinct(id_key)
          end
          count_per_process = (ids.count / Parallel.processor_count.to_f).ceil
          limit = limit_per_process.to_i
          count_per_process = [limit, count_per_process].min if limit > 0

          parallel_each(ids.each_slice(count_per_process)) do |ids_slice|
            # ids.each_slice(count_per_process) do |ids_slice|
            process_ids(node: base_node, ids: ids_slice)
          end
        end

        private

        def process_ids(node:, ids:)
          records = node.all(where: { id_key => ids })
          groups = records.group_by { |x| x[id_key] }

          result = groups.flat_map do |_, group|
            process_group(group)
          end.compact

          data_node.add(records: result)
        end

        # sort the record group and then proceed to drop the elements
        # that satisfy the condition
        def process_group(record_group)
          sort_tokens = record_dig_tokens(key: sort_by, use_sym: dependencies.first.use_symbols?)
          group = record_group.sort_by { |x| x.dig(*sort_tokens) }
          group = group.reverse unless sort_asc
          modes = drop_mode == 'both' ? %w(left right) : [drop_mode]

          modes.each do |mode|
            # if we want to drop on the right,
            # reverse the array, drop on the left and reverse again
            group = group.reverse if mode == 'right'
            group = drop_while(group)
            group = group.reverse if mode == 'right'
          end

          group
        end

        # apply a single drop_while on the group.
        def drop_while(group)
          value_tokens = record_dig_tokens(key: field, use_sym: dependencies.first.use_symbols?)

          case op.to_s.downcase
          when 'eq'
            group.drop_while { |x| x.dig(*value_tokens) == value }
          when 'ne'
            group.drop_while { |x| x.dig(*value_tokens) != value }
          when 'le'
            group.drop_while do |x|
              val = x.dig(*value_tokens)
              next true if val.nil? # drop nil values
              val <= value
            end
          when 'lt'
            group.drop_while do |x|
              val = x.dig(*value_tokens)
              next true if val.nil? # drop nil values
              val < value
            end
          when 'ge'
            group.drop_while do |x|
              val = x.dig(*value_tokens)
              next true if val.nil? # drop nil values
              val >= value
            end
          when 'gt'
            group.drop_while do |x|
              val = x.dig(*value_tokens)
              next true if val.nil? # drop nil values
              val > value
            end
          else
            raise Errors::InvalidConfigurationError, "Invalid op key: #{op}"
          end
        end
      end
    end
  end
end
