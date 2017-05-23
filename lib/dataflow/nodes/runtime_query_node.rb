# frozen_string_literal: true
module Dataflow
  # Interface for a node that behaves as a dataset.
  # Does not support any operation.
  # Inherit and override to implement custom behavior.
  module Nodes
    class RuntimeQueryNode < DataNode

      after_initialize do
        self.db_backend = :none
      end

      def handle_dataset_settings_changed
        # dot not do anything, there is no real dataset
      end

      def all(*_args)
        raise NotImplementedError, 'this node does not support #all'
      end

      def count(*_args)
        raise NotImplementedError, 'this node does not support #count'
      end

      def find(*_args)
        raise NotImplementedError, 'this node does not support #find'
      end

      def all_paginated(*_args)
        raise NotImplementedError, 'this node does not support #all_paginated'
      end

      def add(*_args)
        raise NotImplementedError, 'this node does not support #add'
      end

      def clear(*_args)
        raise NotImplementedError, 'this node does not support #clear'
      end
    end
  end
end
