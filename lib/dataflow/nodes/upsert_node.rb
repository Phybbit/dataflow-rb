# frozen_string_literal: true
module Dataflow
  # Represents a node with a unique index and upsert behavior:
  # If there is any existing that that match on that index,
  # it gets replaced. If not, it simply gets added.
  module Nodes
    class UpsertNode < DataNode
      include Mixin::RenameDottedFields
      include Mixin::AddInternalTimestamp

      before_save :transform_index_key

      field :index_key, required_for_computing: true
      validates_presence_of :index_key

      def set_defaults
        super

        self.indexes ||= []
        # get rid of keys/string confusion
        self.indexes = JSON.parse(self.indexes.to_json)

        # if there is no index_key, take the first unique index
        if index_key.blank?
          first_unique_index = self.indexes.find { |x| x['unique'] }
          self.index_key = (first_unique_index || {})['key']
        end

        # add keys for the unique index keys
        if index_key.present?
          auto_generated_indexes = [{ 'key' => index_key, 'unique' => true }]

          if index_key.is_a? Array
            # generated non-unique indexes for each key in a compound index
            auto_generated_indexes += index_key.map { |idx| { 'key' => idx } }
          end
          self.indexes += auto_generated_indexes
          self.indexes.uniq!
        end

        self.updated_at ||= Time.now
      end

      def add(records:)
        return if records.blank?

        # TODO: create a chain of behavior "before add"
        rename_dotted_fields(records: records)
        add_internal_timestamp(records: records)

        db_adapter.save(records: records, replace_by: index_key)
        self.updated_at = Time.now
        save!
      end

      private

      def transform_index_key
        return unless index_key.is_a?(String)

        # try to split the comma separated string
        keys = index_key.split(',')
        # if there was no comma, leave as-is
        self.index_key = keys if keys.count > 1
      end
    end
  end
end
