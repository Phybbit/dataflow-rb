# frozen_string_literal: true
module Dataflow
  module Nodes
    module Export
      # Export a dataset to CSV
      class ToCsvNode < ComputeNode
        ensure_dependencies exactly: 1

        # A JSON encoded query to pass along.
        field :query, type: String, default: {}.to_json

        def compute_impl
          node = dependencies.first
          where = JSON.parse(query)

          # fetch the schema
          sch = node.infer_partial_schema(where: where, extended: true)

          # re-order the schema if needed
          if node.respond_to? :keys
            sch = node.keys.map { |k| [k, sch[k]] }.to_h if keys.present?
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

          parallel_each(queries.each_with_index) do |query, _idx|
            # TODO: re-enabled event on_export_progressed
            # progress = (idx / queries.count.to_f * 100).ceil
            # on_export_progressed(pct_complete: progress)

            batch = node.all(where: query.merge(where))
            csv_adapter.save(records: batch)
          end

          # needed by the csv exporter to finalize in a single file
          csv_adapter.on_save_finished
        end
      end
    end
  end
end
