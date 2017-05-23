# frozen_string_literal: true
module Dataflow
  module Adapters
    class Settings
      attr_accessor :connection_uri, :db_name,
                    :db_host, :db_port, :db_user, :db_password,
                    :dataset_name, :read_dataset_name, :write_dataset_name,
                    :indexes, :adapter_type, :schema

      def initialize(data_node: nil, connection_uri: nil, db_name: nil,
                     db_host: nil, db_port: nil, db_user: nil, db_password: nil,
                     dataset_name: nil, indexes: nil, adapter_type: nil, schema: nil)
        @connection_uri = connection_uri

        # first try to set the options based on the data node settings
        if data_node.present?
          @db_name            = data_node.db_name
          @db_host            = data_node.db_host
          @db_port            = data_node.db_port
          @db_user            = data_node.db_user
          @db_password        = data_node.db_password
          @dataset_name       = data_node.name
          @read_dataset_name  = data_node.read_dataset_name
          @write_dataset_name = data_node.write_dataset_name
          @indexes            = data_node.indexes
          @schema             = data_node.schema
        end

        # override if needed
        @db_name            ||= db_name
        @db_host            ||= db_host
        @db_port            ||= db_port
        @db_user            ||= db_user
        @db_password        ||= db_password
        @dataset_name       ||= dataset_name
        @read_dataset_name  ||= dataset_name
        @write_dataset_name ||= dataset_name
        @indexes            ||= indexes
        @adapter_type       ||= adapter_type
        @schema             ||= schema
      end

      def set_mongodb_defaults_if_needed!
        @db_host ||= ENV['MOJACO_MONGO_ADDRESS'] || '127.0.0.1'
        @db_port ||= ENV['MOJACO_MONGO_PORT'] || '27017'
        @db_user ||= ENV['MOJACO_MONGO_USER']
        @db_password ||= ENV['MOJACO_MONGO_USER']
      end

      def set_postgresql_defaults_if_needed!
        @db_host ||= ENV['MOJACO_POSTGRESQL_ADDRESS'] || '127.0.0.1'
        @db_port ||= ENV['MOJACO_POSTGRESQL_PORT'] || '5432'
        @db_user ||= ENV['MOJACO_POSTGRESQL_USER']
        @db_password ||= ENV['MOJACO_POSTGRESQL_PASSWORD']
      end

      def set_mysql_defaults_if_needed!
        @db_host ||= ENV['MOJACO_MYSQL_ADDRESS'] || '127.0.0.1'
        @db_port ||= ENV['MOJACO_MYSQL_PORT'] || '3306'
        @db_user ||= ENV['MOJACO_MYSQL_USER']
        @db_password ||= ENV['MOJACO_MYSQL_PASSWORD']
      end

      def connection_uri_or_default
        return @connection_uri if @connection_uri.present?

        send("#{@adapter_type}_default_connection_uri")
      end

      def mongodb_default_connection_uri
        set_mongodb_defaults_if_needed!

        # if user/password are empty, the user_password will be empty as well
        user_password = @db_user
        user_password += ":#{@db_password}" if @db_password.present?
        user_password += '@' if user_password.present?

        # [username:password@]host1[:port1]
        "#{user_password}#{@db_host}:#{@db_port}"
      end

      def mysql_default_connection_uri
        set_mysql_defaults_if_needed!
        sql_default_connection_uri('mysql2')
      end

      def postgresql_default_connection_uri
        set_postgresql_defaults_if_needed!
        sql_default_connection_uri('postgresql')
      end

      def sql_default_connection_uri(scheme)
        user_password = @db_user
        user_password += ":#{@db_password}" if @db_password.present?

        "#{scheme}://#{user_password}@#{@db_host}:#{@db_port}"
      end
    end
  end
end
