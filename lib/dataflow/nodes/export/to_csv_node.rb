# frozen_string_literal: true
module Dataflow
  module Nodes
    module Export
      # Export a dataset to CSV
      class ToCsvNode < ComputeNode
        ensure_dependencies exactly: 1

        # A JSON encoded query to pass along.
        field :query, type: String, default: {}.to_json

        # Which fields to export
        field :keys, type: Array, default: []

        def compute_impl
          node = dependencies.first
          where = JSON.parse(query)

          # fetch the schema
          sch = if keys.present?
                  keys.map { |k| [k, { type: 'string' }] }.to_h
                else
                  node.infer_partial_schema(where: where, extended: true)
                end

          # create the dataset
          csv_adapter = Adapters::CsvAdapter.new(data_node: node)
          csv_adapter.set_schema(sch)
          csv_adapter.recreate_dataset

          # export in parallel
          max_per_process = 1000
          max_per_process = limit_per_process if limit_per_process < 0

          data_count = [node.count(where: where), 1].max
          equal_split_per_process = (data_count / Parallel.processor_count.to_f).ceil
          count_per_process = [max_per_process, equal_split_per_process].min

          queries = node.ordered_system_id_queries(batch_size: count_per_process)
          system_id = node.send(:db_adapter).class::SYSTEM_ID

          parallel_each(queries.each_with_index) do |query, idx|
            # TODO: re-enabled event on_export_progressed
            # progress = (idx / queries.count.to_f * 100).ceil
            # on_export_progressed(pct_complete: progress)
            batch = node.all(where: query.merge(where), fields: sch.keys, sort: { system_id => 1 })
            csv_adapter.save(records: batch, part: idx.to_s.rjust(queries.count.to_s.length, "0"))
          end

          # needed by the csv exporter to finalize in a single file
          csv_adapter.on_save_finished
        end
      end
    end
  end
end
