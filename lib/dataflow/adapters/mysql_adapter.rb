# frozen_string_literal: true
module Dataflow
  module Adapters
    # Interface between a data node and mongodb.
    # We use mongodb to perform all the store/retrieve operations.
    class MysqlAdapter < SqlAdapter
      def fetch_table_usage(dataset:)
        size = client["SELECT data_length + index_length as size from information_schema.TABLES WHERE table_schema = '#{settings.db_name}' and table_name = '#{dataset}'"].first[:size]
        {
          memory: size,
          storage: size
        }
      rescue Sequel::DatabaseError => e
        {
          memory: 0,
          storage: 0
        }
      end
    end
  end
end
