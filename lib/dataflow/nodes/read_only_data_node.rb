# frozen_string_literal: true
module Dataflow
  module Nodes
    # Only supports read operations
    class ReadOnlyDataNode < DataNode

      def set_defaults
        super
        self.use_double_buffering = false
      end


      def handle_dataset_settings_changed
        # ignore - do not do anyhing
      end

      def add(*args)
        raise_read_only_error!
      end

      def clear(*args)
        raise_read_only_error!
      end

      def recreate_dataset(*args)
        raise_read_only_error!
      end

      def create_unique_indexes(*args)
        raise_read_only_error!
      end

      def create_non_unique_indexes(*args)
        raise_read_only_error!
      end

      def read_dataset_name=(*args)
        raise_read_only_error!
      end

      def swap_read_write_datasets!
        raise_read_only_error!
      end

      def import(*args)
        raise_read_only_error!
      end


      def drop_dataset!
        raise_read_only_error!
      end

      private

      def raise_read_only_error!
        raise NotImplementedError, 'External data nodes are read only'
      end

    end # class ExternalDataNode
  end # module Nodes
end # module Dataflow
