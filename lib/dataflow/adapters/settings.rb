# frozen_string_literal: true
module Dataflow
  module Adapters
    class Settings
      attr_accessor :connection_uri, :db_name, :indexes, :adapter_type,
                    :dataset_name, :read_dataset_name, :write_dataset_name, :schema

      def initialize(data_node: nil, connection_uri: nil, db_name: nil,
                     dataset_name: nil, indexes: nil, adapter_type: nil, schema: nil)
        @connection_uri = connection_uri

        # first try to set the options based on the data node settings
        if data_node.present?
          @db_name            = data_node.db_name
          @dataset_name       = data_node.name
          @read_dataset_name  = data_node.read_dataset_name
          @write_dataset_name = data_node.write_dataset_name
          @indexes            = data_node.indexes
          @schema             = data_node.schema
        end

        # override if needed
        @db_name            ||= db_name
        @dataset_name       ||= dataset_name
        @read_dataset_name  ||= dataset_name
        @write_dataset_name ||= dataset_name
        @indexes            ||= indexes
        @adapter_type       ||= adapter_type
        @schema             ||= schema
      end
    end
  end
end
