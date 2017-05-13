# frozen_string_literal: true
module Dataflow
  module Adapters
    # Interface between a data node and mongodb.
    # We use mongodb to perform all the store/retrieve operations.
    class PsqlAdapter < SqlAdapter
      def fetch_table_usage(dataset:)
        size = client["SELECT pg_relation_size('#{dataset}') as size"].first[:size]
        {
          memory: size,
          storage: size
        }
      rescue Sequel::DatabaseError
        {
          memory: 0,
          storage: 0
        }
      end

      def regex_case_senstive_op
        '~'
      end

      def regex_case_insensitive_op
        '~*'
      end
    end
  end
end
