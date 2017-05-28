# frozen_string_literal: true
module Dataflow
  # Interface for a node that behaves as a dataset.
  # Does not support any write operation.
  # Inherit and override to implement custom behavior.
  module Nodes
    class RuntimeQueryNode < ReadOnlyDataNode

      after_initialize do
        self.db_backend = :none
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

    end
  end
end
