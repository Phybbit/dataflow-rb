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
      metadata.select { |k, v| k != :_id && v.is_a?(BSON::ObjectId) }.flat_map { |_, node_id|
          # It's most likely a Node.
          # Let's find out and if it is
          begin
            Dataflow::Node.find(node_id).all_dependencies
          rescue StandardError => _e
            # It seems like it is not a node
            nil
          end
      }.compact + [self]
    end

    def dependency_level(current_level = 0)
      lvl = all_dependencies.map { |node|
        if node == self
          current_level
        else
          node.dependency_level(current_level) + 1
        end
      }.max.to_i

      # if there is any dependency, it will be more than 0, else 0
      lvl
    end

    def required_by
      Dataflow::Nodes::ComputeNode.where(dependency_ids: _id).map { |node|
        { node: node, type: 'dependency' }
      }
    end

    def metadata
      metadata = {
        _id: self._id,
        _type: self._type,
      }
      properties_data = self.class.properties.keys.map do |property_name|
        value = self[property_name]
        [property_name, value]
      end.to_h

      metadata.merge(properties_data)
    end
  end
end
