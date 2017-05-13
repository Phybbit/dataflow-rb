# frozen_string_literal: true
module Dataflow
  module Adapters
    # Interface between a data node and mongodb.
    # We use mongodb to perform all the store/retrieve operations.
    class MongoDbAdapter
      SYSTEM_ID = '_id'

      class << self
        def client(settings, db_name: nil)
          @clients ||= {}
          host = ENV['MOJACO_MONGO_ADDRESS'] || '127.0.0.1'
          port = '27017'
          connection_uri = settings.connection_uri || "#{host}:#{port}"
          db_name ||= settings.db_name
          @clients["#{connection_uri}.#{db_name}"] ||= Mongo::Client.new([connection_uri], database: db_name)
        end

        def admin_client(settings)
          return @admin_client if @admin_client
          @admin_client = client(settings, db_name: 'admin')
        end

        # Force the clients to disconnect their connections.
        # Use before forking.
        def disconnect_clients
          @clients ||= {}
          @clients.values.each(&:close)
        end
      end

      attr_reader :settings
      attr_reader :client

      def initialize(args)
        update_settings(args)
        @client = MongoDbAdapter.client(settings)
        @admin_client = MongoDbAdapter.admin_client(settings)
      end

      def update_settings(args)
        @settings = Dataflow::Adapters::Settings.new(args)
      end

      # retrieve a single element from a data node
      def find(where: {}, fields: [], sort: {}, offset: 0)
        all(where: where, fields: fields, sort: sort, offset: offset, limit: 1).first
      end

      # retrieve all elements from a data node
      def all(where: {}, fields: [], sort: {}, offset: 0, limit: 0)
        projection = fields.map { |field| [field, 1] }

        unless fields.map(&:to_s).include?(SYSTEM_ID)
          # by default, do not select the _id field
          projection << [SYSTEM_ID, 0].freeze
        end

        opts = transform_to_query(where)
        res = client[read_dataset_name].find(opts)
        res = res.projection(projection.to_h)

        res = res.sort(sort)   if sort
        res = res.skip(offset) if offset > 0
        res = res.limit(limit) if limit > 0

        if block_given?
          yield res
        else
          res.to_a
        end
      end

      # Helper that supports paginating through the whole dataset at fixed
      # performance. Unlike using offset/skip which requires to read through
      # the skipped content (high usage of CPU), we use the internal mongo
      # cursor to get batch of results.
      # @return [Hash] with 2 fields: data and next_cursor for the next call
      def all_paginated(where: {}, fields: [], cursor: nil)
        cursor = cursor.to_i
        data = []

        # If there is no cursor, we make the initial query
        # get the first batch of data and get the cursor id.
        if cursor.zero?
          all(where: where, fields: fields) do |res|
            results = res.initial_query
            data = results.documents
            cursor = res.cursor.id
          end
        end

        # The first query's result batch is a small 101 set of results
        # so we want to get one more batch of data.
        # However, there might be queries whose results are very small
        # and the resulting cursor is 0. In such case there is no more
        # data to be fetched.
        unless cursor.zero?
          # send a getMore command on the cursor id
          command = { getMore: cursor, collection: read_dataset_name }
          result = client.database.command(command).documents[0]
          cursor = result['cursor']['id']
          data += result['cursor']['nextBatch']
        end

        # We want to return the cursor as a string.
        # If there is no cursor (zero) then make it empty
        cursor = '' if cursor.zero?

        { 'data' => data, 'next_cursor' => cursor.to_s }
      rescue Mongo::Error::OperationFailure
        { 'data' => data, 'next_cursor' => '' }
      end

      # Create queries that permit processing the whole dataset in parallel without using offsets.
      def ordered_system_id_queries(batch_size:)
        ids = all(fields: [SYSTEM_ID], sort: { SYSTEM_ID => 1 }).map { |x| x[SYSTEM_ID].to_s }
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
        client[read_dataset_name].count(transform_to_query(where))
      end

      # Save the given records.
      # @param replace_by [Array] if the replace_by key is provided,
      #        it will try to replace records with the matching key,
      #        or insert if none is found.
      def save(records:, replace_by: nil)
        if replace_by.present?
          replace_keys = Array(replace_by)
          bulk_ops = records.map do |record|
            filter = replace_keys.map { |x| [x, record[x]] }.to_h
            {
              replace_one: {
                filter: filter,
                replacement: record,
                upsert: true
              }
            }
          end
          client[write_dataset_name].bulk_write(bulk_ops, ordered: false)
        else
          client[write_dataset_name].insert_many(records, ordered: false)
        end
      rescue Mongo::Error::BulkWriteError => e
        dup_key_error = e.result['writeErrors'].all? { |x| x['code'] == 11_000 }
        # don't raise if it is errors about duplicated keys
        unless dup_key_error
          raise e
        end
      end

      # Delete records that match the options.
      # @param where query to apply on the delete operation.
      def delete(where: {})
        client[read_dataset_name].delete_many(transform_to_query(where))
      end

      # recreate the table/collection
      def recreate_dataset(dataset: nil)
        dataset ||= write_dataset_name
        drop_dataset(dataset)
        collection = client[dataset]
        collection.create
      end

      def drop_dataset(dataset)
        collection = client[dataset]
        collection.drop
      end

      # Create the indexes on this dataset.
      # @param dataset [String] Specify on which dataset the operation will be performed.
      #        Default: the adatpter's settings' dataset.
      # @param type [Symbol] select which indexes type to create.
      #        Can be :all (default), :unique_only, :non_unique_only
      def create_indexes(dataset: nil, type: :all, drop_retry_on_error: true)
        dataset ||= write_dataset_name
        return unless settings.indexes.present?

        indexes = (settings.indexes || [])

        case type
        when :unique_only
          indexes = indexes.select { |idx| idx['unique'] }
        when :non_unique_only
          indexes = indexes.reject { |idx| idx['unique'] }
        end

        indexes = indexes.map { |x| format_index(x) }
        client[dataset].indexes.create_many(indexes)
      rescue Mongo::Error::OperationFailure => e
        raise e unless drop_retry_on_error
        client[dataset].indexes.drop_all
        create_indexes(drop_retry_on_error: false)
      end

      def usage(dataset:)
        indexes = retrieve_collection_indexes(dataset)
        command = { collstats: dataset }
        result = client.database.command(command).documents[0]
        {
          memory: result['size'],
          storage: result['storageSize'],
          effective_indexes: indexes
        }
      rescue Mongo::Error::OperationFailure, Mongo::Error::InvalidCollectionName
        {
          memory: 0,
          storage: 0,
          effective_indexes: indexes
        }
      end

      private

      def write_dataset_name
        settings.write_dataset_name
      end

      def read_dataset_name
        settings.read_dataset_name
      end

      def transform_to_query(opts)
        sanitized_opts = {}
        opts.each do |k, v|
          if v.is_a? Array
            # e.g. { 'id' => [1,2] } transform to mongodb IN clauses
            sanitized_opts[k] = { '$in' => v.map { |value| try_cast_value(k, value) } }
          elsif v.is_a? Hash
            sanitized_opts[k] = {}
            v.each do |operator, value|
              case operator.to_s
              when '!='
                # we still need to check and transform into
                if value.is_a? Array
                  # { '$nin' => [value] }
                  sanitized_opts[k]['$nin'] = value.map { |x| try_cast_value(k, x) }
                else
                  # or {'$ne' => value }
                  sanitized_opts[k]['$ne'] = try_cast_value(k, value)
                end
              when '<'
                sanitized_opts[k]['$lt'] = try_cast_value(k, value)
              when '<='
                sanitized_opts[k]['$lte'] = try_cast_value(k, value)
              when '>'
                sanitized_opts[k]['$gt'] = try_cast_value(k, value)
              when '>='
                sanitized_opts[k]['$gte'] = try_cast_value(k, value)
              when '~*' # match regex /regex/i (case insensitive)
                sanitized_opts[k]['$regex'] = /#{value}/i
              when '~'  # match regex /regex/  (case sensitive)
                sanitized_opts[k]['$regex'] = /#{value}/
              end
            end
          else
            sanitized_opts[k] = try_cast_value(k, v)
          end
        end
        sanitized_opts
      end

      def try_cast_value(field, value)
        # cast to time when querying on _mojaco_updated_at
        return Timeliness.parse(value) || value if field =~ /_mojaco_updated_at/
        # cast to ObjectId when querying on _id
        return BSON::ObjectId(value) if field == SYSTEM_ID && value.is_a?(String)

        # TODO: add other casts based on the field type
        value
      end

      # Required index format for mongodb:
      # { :key => { name: 1 }, :unique => true },
      def format_index(dataset_index)
        dataset_index = dataset_index.with_indifferent_access

        index_key = {}
        keys = Array(dataset_index[:key])
        keys.each { |k| index_key[k] = 1 }
        name = keys.map { |k| k[0..1] }.push(SecureRandom.hex(4)).join('_')
        index = { key: index_key, name: name }
        index[:unique] = true if dataset_index[:unique]
        index
      end

      def retrieve_collection_indexes(collection)
        mongo_indexes = client[collection].indexes
        mongo_indexes.map do |idx|
          # skip the default index
          next if idx['key'].keys == ['_id']

          index = { 'key' => idx['key'].keys }
          index['unique'] = true if idx['unique']
          index
        end.compact
      end
    end
  end
end
