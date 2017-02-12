# frozen_string_literal: true
module Dataflow
  module Nodes
    # Performs a join operation on 2 dependencies.
    class JoinNode < ComputeNode
      VALID_TYPES = %w(inner left).freeze
      field :join_type,  type: String, required_for_computing: true, values: VALID_TYPES, default: VALID_TYPES[0]
      field :key1,       type: String, required_for_computing: true
      field :key2,       type: String, required_for_computing: true
      # Support joining on multiple keys by setting them in the other keys.
      # other_keys_1 and 2 must match in length
      field :other_keys1,  type: Array, default: []
      field :other_keys2,  type: Array, default: []
      field :prefix1,       type: String, default: ''
      field :prefix2,       type: String, default: ''

      ensure_data_node_exists
      ensure_dependencies exactly: 2

      def valid_for_computation?
        # We need an equivalent number of keys as they will be matched with each others
        if other_keys1.count != other_keys2.count
          errors.add(:other_keys2, "#{self.class} other_keys2 must match other_keys1's length")
        end

        super
      end

      def required_schema
        return {} unless dependencies.count == 2

        # merge both dependencies schemas
        sch = dependencies.first.schema || {}
        sch.merge(dependencies.second.schema || {})
      end

      def compute_impl
        all_same_postgresql = db_backend == :postgresql
        all_same_postgresql &&= dependencies[1..-1].all? do |dep|
          dep.db_backend == :postgresql && dep.db_name == db_name
        end

        if all_same_postgresql
          # use SQL join
          execute_sql_join
          self.updated_at = Time.now
        else
          # use software join
          super
        end
      end

      private

      def execute_sql_join
        fields = required_schema.keys
        select_keys = dependencies[0].schema.keys.map { |x| "d1.#{x}" } + (dependencies[1].schema.keys - dependencies[0].schema.keys).map { |x| "d2.#{x}" }
        query = "INSERT INTO #{write_dataset_name} (#{fields.join(',')})
                 SELECT #{select_keys.join(', ')}
                 FROM #{dependencies[0].read_dataset_name} as d1
                 INNER JOIN #{dependencies[1].read_dataset_name} as d2
                 ON d1.#{key1} = d2.#{key2}"
        p query
        db_adapter.client[query].to_a
      end

      def compute_batch(records:)
        join(n1_records: records)
      end

      def join(n1_records:)
        tokens_key1 = record_dig_tokens(key: key1, use_sym: dependencies.first.use_symbols?)
        tokens_key2 = record_dig_tokens(key: key2, use_sym: dependencies.second.use_symbols?)
        other_tokens_key1 = (other_keys1 || []).map do |key|
          record_dig_tokens(key: key, use_sym: dependencies.second.use_symbols?)
        end
        other_tokens_key2 = (other_keys2 || []).map do |key|
          record_dig_tokens(key: key, use_sym: dependencies.second.use_symbols?)
        end

        # fetch necessary records from node2
        node2 = dependencies.second
        n2_ids = n1_records.map { |x| x.dig(*tokens_key1) }.compact.uniq
        n2_records = node2.all(where: { key2 => n2_ids })

        # preload and map dataset2 by the key we want to lookup
        mapped_data2 = {}
        if has_multiple_keys?
          n2_records.each do |datum2|
            lookup_value = datum2.dig(*tokens_key2)
            mapped_data2[lookup_value] ||= []
            mapped_data2[lookup_value] << datum2
          end
        else
          n2_records.each do |datum2|
            lookup_value = datum2.dig(*tokens_key2)
            mapped_data2[lookup_value] = datum2
          end
        end

        # for each datum in dataset1, find the corresponding datum in dataset2
        n1_records.map do |d1|
          join_value = d1.dig(*tokens_key1)
          next if join_value.nil?

          d2 = mapped_data2[join_value]
          if has_multiple_keys? && !d2.nil?
            # in this case, it will be an array,
            # so we need to further search the correct datum
            d2 = find_matching_record(d1, d2, other_tokens_key1, other_tokens_key2)
          end

          # if there is no d2, only continue based on the type of join we want.
          next if d2.blank? && join_type == 'inner'

          # there might be the case that nothing was found after-all
          d2 ||= {}

          # prefix if needed
          d1 = Hash[d1.map { |k, v| ["#{prefix1}#{k}", v] }] if prefix1.present?
          d2 = Hash[d2.map { |k, v| ["#{prefix2}#{k}", v] }] if prefix2.present?

          d1.reverse_merge(d2)
        end.compact
      end

      def has_multiple_keys?
        other_keys1.present? && other_keys2.present?
      end

      # Find a record in d2_list that can be join with d1 based on
      # the values in the fields specified in other_keys_1/2.
      # @param d1 [Hash] a datum
      # @param d2_list [Array] an array of datums that may match with d1
      # @param other_keys1 [Array] an array of arrays (tokens) that will
      #        be used to fetch the corresponding value in d1
      # @param other_keys2 [Array] an array of arrays (tokens) that will
      #        be used to fetch the corresponding value in the d2_list
      # @return [Hash] a record if found, nil otherwise.
      def find_matching_record(d1, d2_list, other_tokens1, other_tokens2)
        values1 = other_tokens1.map { |tokens| d1.dig(*tokens) }
        d2_list.find do |d2|
          values1.each_with_index.all? do |value1, idx|
            # does this record match d1 on all the fields in other_key1/2?
            value1 == d2.dig(*(other_tokens2[idx]))
          end
        end
      end
    end
  end
end
