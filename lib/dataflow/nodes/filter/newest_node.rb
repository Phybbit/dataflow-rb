# frozen_string_literal: true
module Dataflow
  module Nodes
    module Filter
      # Select the newest record among records with the same id key.
      class NewestNode < ComputeNode
        field :id_key, type: String, required_for_computing: true
        field :date_key, type: String, required_for_computing: true

        ensure_data_node_exists
        ensure_dependencies exactly: 1

        private

        def ensure_keys_are_set!
          raise Errors::InvalidConfigurationError, 'Id key must be set.' if id_key.blank?
          raise Errors::InvalidConfigurationError, 'Date key must be set.' if date_key.blank?
        end

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

        def process_ids(node:, ids:)
          metatata = node.all(where: { id_key => ids }, fields: [id_key, date_key])
          groups = metatata.group_by { |x| x[id_key] }
          newest_record_metadata = filter_by_newest(groups: groups,
                                                    date_key: date_key)
          records = newest_record_metadata.map do |metadata|
            query = {
              id_key => metadata[id_key],
              date_key => metadata[date_key]
            }
            node.find(where: query)
          end.compact

          data_node.add(records: records)
        end

        def filter_by_newest(groups:, date_key:)
          groups.map do |_, entries|
            # sort by date ASC and select the newest
            entries
              .sort_by do |x|
              x[date_key].is_a?(Time) ? x[date_key] : Timeliness.parse(x[date_key])
            end.last
          end
        end
      end
    end
  end
end
