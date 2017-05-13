# frozen_string_literal: true
module Dataflow
  module Adapters
    # Interface between a data node and mongodb.
    # We use mongodb to perform all the store/retrieve operations.
    class SqlAdapter
      class << self
        # Get (or create) a client that satisfies the given connection settings.
        # @param settings [Hash] Represents the connection settings to the DB.
        # @param db_name [String] The database name to which the client will connect.
        # @return [Sequel::Database] a sequel database object.
        def client(settings, db_name: nil)
          @clients ||= {}

          case settings.adapter_type
          when 'mysql2'
            host = ENV['MOJACO_MYSQL_ADDRESS'] || '127.0.0.1'
            port = ENV['MOJACO_MYSQL_PORT'] || '3306'
            user = ENV['MOJACO_MYSQL_USER']
            password = ENV['MOJACO_MYSQL_PASSWORD']
          when 'postgresql'
            host = ENV['MOJACO_POSTGRESQL_ADDRESS'] || '127.0.0.1'
            port = ENV['MOJACO_POSTGRESQL_PORT'] || '5432'
            user = ENV['MOJACO_POSTGRESQL_USER']
            password = ENV['MOJACO_POSTGRESQL_PASSWORD']
          end

          db_name ||= settings.db_name
          user_password = user
          user_password += ":#{password}" if password.present?

          uri = "#{settings.adapter_type}://#{user_password}@#{host}:#{port}"
          connection_uri = settings.connection_uri || "#{uri}/#{db_name}"

          return @clients[connection_uri] if @clients[connection_uri].present?

          # first, make sure the DB is created (if it is not an external db)
          is_external_db = settings.connection_uri.present?
          try_create_db(uri, db_name, user, password) unless is_external_db

          # then, create the connection object
          db = Sequel.connect("#{connection_uri}?encoding=utf8")
          add_extensions(settings, db)
          @clients[connection_uri] = db
        end

        # Used internally to try to create the DB automatically.
        # @param uri [String] the connection uri to the DB.
        # @param db_name [String] the database name.
        # @return [Boolean] whether the db was created or not.
        def try_create_db(uri, db_name, user, password)
          Sequel.connect(uri, user: user, password: password) do |db|
            db.run("CREATE DATABASE #{db_name}")
            true
          end
        rescue Sequel::DatabaseError => e
          # ignore error
          false
        end

        # load Sequel extensions based on the type
        def add_extensions(settings, db)
          if settings.adapter_type == 'postgresql'
            db.extension(:pg_loose_count)
          end
        end

        # Force the clients to disconnect their connections.
        # Use before forking.
        def disconnect_clients
          @clients ||= {}
          @clients.values.each(&:disconnect)
        end
      end

      SYSTEM_ID = :_id

      attr_reader :settings
      attr_reader :client

      def initialize(args)
        update_settings(args)
        @client = SqlAdapter.client(settings)
      end

      def update_settings(args)
        @settings = Dataflow::Adapters::Settings.new(args)
        @schema = @settings.schema
      end

      # retrieve a single element from a data node
      def find(where: {}, fields: [], sort: {}, offset: 0)
        all(where: where, fields: fields, sort: sort, offset: offset, limit: 1).first
      end

      # retrieve all elements from a data node
      def all(where: {}, fields: [], sort: {}, offset: 0, limit: 0)
        res = client[settings.read_dataset_name.to_sym]

        # if there is no fields, automatically
        # select all the fields expect the system _id
        fields = res.columns.reject { |x| x == SYSTEM_ID } if fields.blank?

        res = res.select(*fields.map(&:to_sym)) if fields.present?
        res = apply_query(res, where)

        (sort || {}).each do |k, v|
          sort_value = v == 1 ? k.to_sym : Sequel.desc(k.to_sym)
          res = res.order(sort_value)
        end

        res = res.offset(offset) if offset > 0
        res = res.limit(limit) if limit > 0

        if block_given?
          yield res
        else
          res.to_a
        end
      end

      def all_paginated(where: {}, fields: [], cursor: nil)
        # for now, retrieve all records at once
        { 'data' => all(where: where, fields: fields), 'next_cursor' => '' }
      end

      # Create queries that permit processing the whole dataset in parallel without using offsets.
      def ordered_system_id_queries(batch_size:)
        ids = all(fields: [SYSTEM_ID], sort: { SYSTEM_ID => 1 }).map { |x| x[SYSTEM_ID] }
        queries_count = (ids.size / batch_size.to_f).ceil
        Array.new(queries_count) do |i|
          from = ids[i * batch_size]
          to = ids[(i + 1) * batch_size] || ids[-1]
          is_last = i == queries_count - 1

          where_query = { SYSTEM_ID => { '>=' => from } }
          operator = is_last ? '<=' : '<'
          where_query[SYSTEM_ID][operator] = to

          where_query
        end
      end

      # count the number of records
      def count(where: {})
        res = client[settings.read_dataset_name.to_sym]
        res = apply_query(res, where)
        res.count
      rescue Sequel::DatabaseError
        0
      end

      # Save the given records
      # @param replace_by [Array] if the replace_by key is provided,
      #        it will try to replace records with the matching key,
      #        or insert if none is found.
      #        NOTE: the replace_by keys must be UNIQUE indexes.
      def save(records:, replace_by: nil)
        dataset_name = settings.write_dataset_name.to_sym
        dataset = client[dataset_name]
        columns = dataset.columns.reject { |x| x == SYSTEM_ID }

        tabular_data = records.map do |record|
          columns.map { |col| record[col] }
        end

        if replace_by.present?
          index_keys = Array(replace_by).map { |c| c.to_sym}.uniq

          # On conflict update every field. On Postgresql we can refer
          # to the "conflicting" rows using the "excluded_" prefix:
          update_clause = columns.map { |k| [k, Sequel.qualify('excluded', k)] }.to_h
          dataset
            .insert_conflict(target: index_keys, update: update_clause)
            .import(columns, tabular_data)
        else
          # ignore insert conflicts
          dataset.insert_ignore.import(columns, tabular_data)
        end
      end

      # Delete records that match the options.
      # @param where query to apply on the delete operation.
      # @note this deletes on the read dataset
      # i.e. changes are seen immediately in the case of double buffered datasets
      def delete(where: {})
        res = client[settings.read_dataset_name.to_sym]
        res = apply_query(res, where)
        res.delete
      end

      # recreate the table/collection
      def recreate_dataset(dataset: nil)
        dataset ||= settings.write_dataset_name.to_sym
        drop_dataset(dataset)
        create_table(dataset, @schema)
      end

      # drops the given dataset
      def drop_dataset(dataset)
        client.drop_table?(dataset)
      end

      # Create the indexes on this dataset.
      # @param dataset [String] Specify on which dataset the operation will be performed.
      #        Default: the adatpter's settings' dataset.
      # @param type [Symbol] select which indexes type to create.
      #        Can be :all (default), :unique_only, :non_unique_only.
      # TODO: add support for a :drop_retry_on_error parameter.
      def create_indexes(dataset: nil, type: :all)
        dataset ||= settings.write_dataset_name
        dataset = dataset.to_sym
        indexes = (settings.indexes || [])

        case type
        when :unique_only
          indexes = indexes.select { |idx| idx['unique'] }
        when :non_unique_only
          indexes = indexes.reject { |idx| idx['unique'] }
        end

        indexes.each do |index|
          params = index_parameters(index)

          begin
            client.add_index(dataset, *params)
          rescue Sequel::DatabaseError => e
            # ignore index already exists
            next if e.wrapped_exception.is_a?(PG::DuplicateTable)

            # log columns not found but do not raise an error
            if e.wrapped_exception.is_a?(PG::UndefinedColumn)
              logger.error(custom_message: "add_index on #{dataset} failed.", error: e)
              next
            end

            # re-raise for everything else
            raise e
          end
        end
      end

      def usage(dataset:)
        indexes = retrieve_collection_indexes(dataset)
        table_usage = fetch_table_usage(dataset: dataset)
        table_usage.merge(effective_indexes: indexes)
      end

      private

      MAX_INT = 2_147_483_647
      MAX_VARCHAR = 255

      def create_table(dataset, schema)
        client.create_table(dataset.to_sym) do
          # always add an _id field to be used internally
          primary_key SYSTEM_ID

          schema.each do |column, info|
            type = info[:type]
            max_size = info[:max] || info.dig(:types, type, :max)

            case type
            when 'object', 'string'
              max_size ||= info.dig(:types, 'string', :max) || MAX_VARCHAR + 1
              col_type = if max_size <= MAX_VARCHAR
                           "varchar(#{max_size})"
                         else
                           'text'
                         end
            when 'time'
              col_type = 'timestamp'
            when 'datetime'
              col_type = 'timestamp with time zone'
            when 'integer'
              max_size ||= MAX_INT + 1
              col_type = if max_size <= MAX_INT
                           'integer'
                         else
                           'bigint'
                         end
            when 'numeric'
              col_type = 'real'
            when 'array', 'hash'
              logger.log("Check type of field #{column} (given: #{type}). Not expecting to use JSON.")
              col_type = 'json'
            when 'date', 'time'
              # keep as-is
              col_type = type
            else
              logger.log("[Error] unexpected type '#{type}'. Keeping as-is.")
              col_type = type
            end

            # create a column with the given type
            p "#{column} #{type} -> #{col_type}"
            column(column.to_sym, col_type)
          end
        end
      end

      def apply_query(res, opts)
        queries = transform_to_query(opts)
        queries.each do |query_args|
          res = res.where(*query_args)
        end
        res
      end

      def transform_to_query(opts)
        # map to a serie of AND clauses queries
        opts.flat_map do |k, v|
          if v.is_a? Hash
            v.map do |operator, value|
              case operator
              when '!='
                if value.is_a? Array
                  Sequel.lit("#{k} NOT IN ?", value)
                else
                  Sequel.lit("#{k} <> ?", value)
                end
              when '<', '<=', '>', '>=', '~', '~*'
                Sequel.lit("#{k} #{operator} ?", value)
              end
            end
          else
            # e.g. simple match { 'id' => 1} or IN clauses { 'id' => [1,2] }
            # are supported with simples hashes
            [[{ k.to_sym => v }]]
          end
        end
      end

      # Required index format for sequel:
      # :keys, unique: true
      def index_parameters(index)
        index = index.with_indifferent_access
        keys = Array(index[:key]).map(&:to_sym)
        params = [keys]
        params << { unique: true } if index[:unique]
        params
      end

      def retrieve_collection_indexes(dataset_name)
        psql_indexes = client.indexes(dataset_name)
        psql_indexes.values.map do |idx|
          cols = idx[:columns].map(&:to_s)
          index = { 'key' => cols }
          index['unique'] = true if idx[:unique]
          index
        end.compact
      rescue Sequel::DatabaseError
        []
      end

      def logger
        @logger ||= Dataflow::Logger.new(prefix: "Dataflow[#{settings.dataset_name}]")
      end
    end
  end
end
