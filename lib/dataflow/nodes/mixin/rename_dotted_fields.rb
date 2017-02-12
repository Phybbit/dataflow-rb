# frozen_string_literal: true
module Dataflow
  module Nodes
    module Mixin
      # Support tranversing the record and rename fields that contain a dot '.'.
      module RenameDottedFields
        # Add a mixin-specific field to the node
        def self.included(base)
          base.class_eval do
            field :rename_dotted_fields_in, type: Array
          end
        end

        # Rename the specified dotted fields
        def rename_dotted_fields(records:)
          return if rename_dotted_fields_in.blank?

          traverse_whole_record = rename_dotted_fields_in.include?('.')

          records.each do |record|
            if traverse_whole_record
              traverse_and_rename_dotted_fields(record)
            else
              rename_dotted_fields_in.each do |field|
                value = record[field]
                if value.is_a?(Array)
                  traverse_and_rename_dotted_fields_in_array(value)
                elsif value.is_a?(Hash)
                  traverse_and_rename_dotted_fields(value)
                end
              end
            end
          end
        end

        # Traverse a hash and look for the fields to rename
        def traverse_and_rename_dotted_fields(hash)
          return if hash.blank?

          hash.keys.each do |k|
            value = hash[k]
            if value.is_a?(Array)
              traverse_and_rename_dotted_fields_in_array(value)
            elsif value.is_a?(Hash)
              traverse_and_rename_dotted_fields(value)
            end

            next unless k.include?('.')
            hash[k.tr('.', '_')] = value
            hash.delete(k)
          end
        end

        # Looks for hashs in the array that may require a transformation
        def traverse_and_rename_dotted_fields_in_array(array)
          array.each do |v|
            traverse_and_rename_dotted_fields(v) if v.is_a?(Hash)
          end
        end
      end # module RenameDottedFields
    end # module Mixin
  end # module Nodes
end # module Dataflow
