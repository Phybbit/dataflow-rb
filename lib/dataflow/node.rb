# frozen_string_literal: true
module Dataflow
  # Define (default) common interface for nodes.
  # These may be overriden with their specific implementations.
  module Node
    # Returns either a DataNode or a ComputeNode that match the id
    def self.find(id)
      begin
        return Dataflow::Nodes::DataNode.find(id)
      rescue Mongoid::Errors::DocumentNotFound
        # try again against a computed node
      end

      Dataflow::Nodes::ComputeNode.find(id)
    end

    def recompute(*args)
      # Interface only, for recursion purposes
    end

    # Overriden in computed node
    def valid_for_computation?
      true
    end

    def validate!
      # throw if normal model validation do not pass.
      valid = valid_for_computation?
      raise Dataflow::Errors::InvalidConfigurationError, errors.messages unless valid
      true
    end

    def all_dependencies
      []
    end

    def required_by
      Dataflow::Nodes::ComputeNode.where(dependency_ids: _id).map { |node|
        { node: node, type: 'dependency' }
      }
    end
  end
end
