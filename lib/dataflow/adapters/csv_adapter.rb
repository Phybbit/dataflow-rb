# frozen_string_literal: true
require 'securerandom'

module Dataflow
  module Adapters
    # Interface between a data node and csv.
    # We use mongodb to perform all the store/retrieve operations.
    class CsvAdapter
      include Dataflow::SchemaMixin

      attr_reader :settings

      def initialize(args)
        # make sure the CsvPath exist
        `mkdir -p #{Dataflow::CsvPath}`
        update_settings(args)
      end

      def update_settings(args)
        @settings = Dataflow::Adapters::Settings.new(args)
        @schema = [] # TODO: pre-fetch the csv's schema
      end

      def set_schema(schema)
        @schema = schema
      end

      # retrieve a single element from a data node
      def find(where: opts = {})
        raise NotImplementedError, '#find is not yet support on CSV.'
      end

      # retrieve all elements from a data node
      def all(where: {}, fields: [], sort: {}, offset: 0, limit: 0, include_system_id: false)
        SmarterCSV.process(file_path, strings_as_keys: true)
      rescue Errno::ENOENT => e
        []
      end

      # count the number of records
      def count(where: {})
        all(where: where).count
      end

      # save the given records
      def save(records:, part: nil)
        write_csv_part(records, keys: @schema.keys, part: part)
      end

      def on_save_finished
        write_single_csv(keys: @schema.keys)
      end

      def remove(_opts = {})
        raise NotImplementedError, '#find is not yet support on CSV.'
      end

      def recreate_dataset(dataset: nil)
        # simply delete the file
        delete_file(file_path)
        # and any parts if any is still there
        file_parts.each { |part| delete_file(part) }
      end

      def create_indexes(*); end

      private

      def delete_file(path)
        File.delete(path)
      rescue Errno::ENOENT => e
        # no file present, no problem
      end

      def file_path
        filename = "#{settings.db_name}.#{settings.dataset_name}.csv"
        "#{Dataflow::CsvPath}/#{filename}"
      end

      def file_parts
        part = "#{settings.db_name}.#{settings.dataset_name}.csv.part_"
        Dir["#{file_path}.part_*"].sort
      end

      def write_csv_part(data, keys:, part:)
        # prepare the data
        key_tokens = keys.map { |key| record_dig_tokens(key: key) }
        rows = data.map do |datum|
          key_tokens.map { |tokens| serialize(datum.dig(*tokens)) }
        end

        # dump in a part file
        part ||= SecureRandom.hex
        CSV.open("#{file_path}.part_#{part}", 'w') do |csv|
          rows.each { |row| csv << row }
        end
      end

      def write_single_csv(keys:)
        # export headers
        header_filepath = "#{file_path}.header"
        CSV.open(header_filepath, 'w') do |csv|
          csv << keys
        end

        # make sure the destination file is deleted
        delete_file(file_path)

        # merge the files into the output
        files = [header_filepath] + file_parts
        files.each do |file|
          # cat each file to the destination file
          `cat #{file} >> #{file_path}`
        end

        # remove the intermediary files
        files.each do |file|
          delete_file(file)
        end
      end

      def serialize(value)
        case value
        when Time, DateTime, Date
          value.iso8601
        else
          value
        end
      end

    end
  end
end
