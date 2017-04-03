# frozen_string_literal: true
module Dataflow
  module SchemaMixin
    SEPARATOR = '|' # if this change, update the regex that use the character directly in this mixin

    # Generate a schema based on this collection's records.
    # We evaluate the schema of each record and then merge all
    # the information together.
    # @param extended [Boolean] Set to true to keep each field as a basic type.
    #        Set to false to reduce the terminal arrays to a single key (under the type array).
    # @return [Hash] with one entry per 'column'/'field'. The values
    #         contains information about the type and usage.
    def infer_schema(samples_count: 0, extended: false)
      if db_backend == :postgresql
        # Experimental
        sch = db_adapter.client.schema(read_dataset_name).to_h
        sch = sch.reject{ |k, v| k == :_id }.map { |k,v| [k, {type: v[:type].to_s}] }.to_h
        self.inferred_schema = sch
        save
        return sch
      end

      data_count = samples_count == 0 ? count : samples_count # invoked in the base class
      return {} if data_count == 0

      # find out how many batches are needed
      max_per_process = 1000
      max_per_process = limit_per_process if respond_to?(:limit_per_process) && limit_per_process > 0

      equal_split_per_process = (data_count / Parallel.processor_count.to_f).ceil
      count_per_process = [max_per_process, equal_split_per_process].min

      queries = ordered_system_id_queries(batch_size: count_per_process)[0...data_count]

      self.inferred_schema_at = Time.now
      self.inferred_schema_from = samples_count
      on_schema_inference_started

      sch = schema_inferrer.infer_schema(batch_count: queries.count, extended: extended) do |idx|
        progress = (idx / queries.count.to_f * 100).ceil
        on_schema_inference_progressed(pct_complete: progress)

        all(where: queries[idx])
      end

      self.inferred_schema = sch
      save
      on_schema_inference_finished

      sch
    end

    def infer_partial_schema(where:, extended: false)
      data_count = count(where: where)
      return {} if data_count == 0

      max_per_process = 250
      max_per_process = limit_per_process if respond_to? :limit_per_process

      equal_split_per_process = (data_count / Parallel.processor_count.to_f).ceil
      count_per_process = [max_per_process, equal_split_per_process].min

      queries = ordered_system_id_queries(batch_size: count_per_process)

      sch = schema_inferrer.infer_schema(batch_count: queries.count, extended: extended) do |idx|
        all(where: queries[idx].merge(where))
      end
    end

    def schema_inferrer
      Schema::Inference::SchemaInferrer.new(
        separator: SEPARATOR,
        convert_types_to_string: true
      )
    end

    SAMPLE_DATA_OUTPUT = %w(raw tabular).freeze
    # Outputs sample data. Support either output raw data (as-is) tabular data.
    def sample_data(count: 5, mode: 'tabular')
      mode = mode.to_s.downcase
      unless SAMPLE_DATA_OUTPUT.include?(mode)
        raise Errors::InvalidConfigurationError, "Mode must be one of '#{SAMPLE_DATA_OUTPUT.join(', ')}'. Given: #{mode}"
      end
      samples = all { |x| x.limit(count) }.to_a
      return samples if mode == 'raw'
      return {} if samples.count == 0

      # tabular output
      schm = schema_inferrer.infer_schema(dataset: samples, extended: true)
      keys = schm.keys
      res = samples.map do |sample|
        keys.map do |key|
          value = record_value(record: sample, key: key)
          next if value.nil?
          [key, value]
        end.compact.to_h
      end

      res
    end

    private

    def record_dig_tokens(key:, use_sym: false)
      return [key] unless key.is_a?(String)
      key.split(SEPARATOR).map do |token|
        # only parse integers for array indexing
        next token.to_i if is_integer?(token)
        next token.to_sym if use_sym
        token
      end
    end

    def record_value(record:, key:)
      tokens = record_dig_tokens(key: key)
      record.dig(*tokens)
    end

    def add_value_to_record(record:, key:, value:)
      tokens = key.is_a?(String) ? key.split(SEPARATOR) : [key]
      current_ref = record
      previous_token = tokens[0]

      tokens[1..-1].each_with_index do |token|
        if is_integer?(token)
          current_ref[previous_token] ||= []
          current_ref = current_ref[previous_token]
          previous_token = token.to_i
        else
          current_ref[previous_token] ||= {}
          current_ref = current_ref[previous_token]
          previous_token = token
        end
      end

      current_ref[previous_token] = value
    end

    def is_integer?(value)
      (/^[+-]?[0-9]+$/ =~ value).present?
    end
  end
end
