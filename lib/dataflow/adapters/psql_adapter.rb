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

      def dump(base_folder:)
        archive_path = "#{base_folder}/#{@settings.db_name}.#{@settings.dataset_name}.dump"
        options = "--table=public.#{@settings.read_dataset_name}"
        options += "--host=#{@settings.db_host}" if @settings.db_host.present?
        options += "--port=#{@settings.db_port}" if @settings.db_port.present?
        options += "--username=#{@settings.db_user}" if @settings.db_user.present?
        password = "PGPASSWORD=#{@settings.db_password} " if @settings.db_password.present?
        `mkdir -p #{base_folder}`
        `#{password}pg_dump #{options} -Fc #{@settings.db_name} > #{archive_path}`
        archive_path
      end

      def restore(filepath:)
        options = "--table=#{@settings.read_dataset_name}"
        options += "--host=#{@settings.db_host}" if @settings.db_host.present?
        options += "--port=#{@settings.db_port}" if @settings.db_port.present?
        options += "--username=#{@settings.db_user}" if @settings.db_user.present?
        password = "PGPASSWORD=#{@settings.db_password} " if @settings.db_password.present?
        p "#{password}pg_restore #{options} -Fc --dbname=#{@settings.db_name} #{filepath}"
        `#{password}pg_restore #{options} -Fc --dbname=#{@settings.db_name} #{filepath}`
      end
    end
  end
end
