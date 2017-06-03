# frozen_string_literal: true
module Dataflow
  # Represents a node that captures changes over time.
  module Nodes
    # TODO: extend the unique node?
    class SnapshotNode < DataNode
      include Mixin::RenameDottedFields
      include Mixin::AddInternalTimestamp

      field :index_key, type: String, required_for_computing: true
      field :updated_at_key, type: String, required_for_computing: true

      validates_presence_of :index_key
      validates_presence_of :updated_at_key

      def set_defaults
        super

        self.indexes ||= []
        # get rid of keys/string confusion
        self.indexes = JSON.parse(self.indexes.to_json)

        # add keys for the index, updated_at and unique keys
        self.indexes += [{ 'key' => index_key }] if index_key
        self.indexes += [{ 'key' => updated_at_key }] if updated_at_key
        self.indexes += [{ 'key' => [index_key, updated_at_key], 'unique' => true }] if index_key && updated_at_key
        self.indexes.uniq!

        self.updated_at ||= Time.now
      end

      def add(records:)
        raise ArgumentError, "records must be an array of documents. Received: '#{records.class}'." unless records.is_a?(Array)
        records = records.compact
        return if records.blank?

        # TODO: create a chain of behavior "before add"
        rename_dotted_fields(records: records)
        add_internal_timestamp(records: records)

        records.delete_if do |record|
          convert_update_at_key(record)
          is_record_redundant?(record: record)
        end.compact
        super(records: records)
      end

      private

      # If this record already exists, and only the updated_at
      # key changed, but the rest of the content is the same,
      # we will consider it to be redundant
      def is_record_redundant?(record:)
        id = record[index_key]
        previous_record = db_adapter.find(where: { index_key => id },
                                          sort: { updated_at_key => -1 })
        return false if previous_record.blank?

        has_same_content = previous_record.keys == record.keys
        has_same_content &&= previous_record.keys.all? do |k|
          # we allow the updated_at key to change, or the mojaco time stamp
          next true if k == updated_at_key || k == internal_timestamp_key
          # but most importantly, the rest of the content should be the same
          record[k] == previous_record[k]
        end

        has_same_content
      end

      def convert_update_at_key(record)
        return if record[updated_at_key].is_a?(Time)

        # try to parse as a string
        record[updated_at_key] = Time.parse(record[updated_at_key])
      rescue TypeError
        # try to parse as a timestamp
        record[updated_at_key] = Time.at(record[updated_at_key])
      end
    end
  end
end
