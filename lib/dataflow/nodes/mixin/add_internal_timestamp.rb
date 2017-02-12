# frozen_string_literal: true
module Dataflow
  module Nodes
    module Mixin
      # Add an internal updated_at timestamp to the records.
      module AddInternalTimestamp
        def self.included(base)
          base.class_eval do
            field :use_internal_timestamp, type: Boolean, default: true
            field :internal_timestamp_key, type: String, default: '_mojaco_updated_at'
          end
        end

        # Add an internal updated_at timestamp to the records
        def add_internal_timestamp(records:)
          return unless use_internal_timestamp
          return unless internal_timestamp_key.present?

          updated_at = Time.now
          records.each do |record|
            record[internal_timestamp_key] = updated_at
          end
        end
      end # module AddInternalTimestamp
    end # module Mixin
  end # module Nodes
end # module Dataflow
