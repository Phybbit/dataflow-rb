# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::MapNode, type: :model do
  describe '#validate!' do
    it 'throws if there is not 2 dependencies' do
      expect { not_enough_deps_node_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'maps keys and values' do
      base_node.add(records: dataset)
      mapping_node.add(records: mapping_dataset)

      map_node.recompute
      expect_dataset_are_equal(map_node.all, expected_dataset, key: 'id')
    end

    it 'supports default values' do
      base_node.add(records: dataset)
      mapping_node.add(records: mapping_with_default)

      map_node.recompute
      expect_dataset_are_equal(map_node.all, expected_dataset_with_default, key: 'id')
    end

    it 'supports lambdas' do
      base_node.add(records: dataset)
      mapping_node.add(records: mapping_with_lambda)

      map_node.recompute
      expect_dataset_are_equal(map_node.all, expected_dataset_with_lambda, key: 'id')
    end

    it 'supports mapping both key and value' do
      base_node.add(records: mapped_key_dataset)
      mapping_node.add(records: mapping_with_mapped_key)

      map_node.recompute
      expect_dataset_are_equal(map_node.all, expected_dataset_with_mapped_key, key: 'id')
    end
  end

  let (:base_node) do
    make_data_node('data')
  end
  let (:mapping_node) do
    make_data_node('mapping')
  end
  let (:map_node) do
    Dataflow::Nodes::MapNode.create(
      name: 'map_node',
      dependency_ids: [base_node, mapping_node],
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset) do
    [{
      'id' => 1,
      'key_1' => 'value1'
    },
     {
       'id' => 2,
       'key_1' => 'value2'
     }]
  end

  let (:mapping_dataset) do
    [{
      'key' => 'key_1',
      'mapped_key' => 'mapped_key_1',
      'values' => {
        'value1' => 'mapped_value1'
      }
    }]
  end

  let (:expected_dataset) do
    [{
      'id' => 1,
      'key_1' => 'value1',
      'mapped_key_1' => 'mapped_value1'
    },
     {
       'id' => 2,
       'key_1' => 'value2',
       'mapped_key_1' => 'value2'
     }]
  end

  let (:mapping_with_lambda) do
    [{
      'key' => 'key_1',
      'values' => 'lambda { |x| "mapped_#{x}" }'
    }]
  end

  let (:expected_dataset_with_lambda) do
    [{
      'id' => 1,
      'key_1' => 'mapped_value1'
    },
     {
       'id' => 2,
       'key_1' => 'mapped_value2'
     }]
  end

  let (:mapping_with_default) do
    [{
      'key' => 'key_1',
      'values' => {
        'value1' => 'mapped_value1'
      },
      'default' => 'not mapped'
    }]
  end

  let (:expected_dataset_with_default) do
    [{
      'id' => 1,
      'key_1' => 'mapped_value1'
    },
     {
       'id' => 2,
       'key_1' => 'not mapped'
     }]
  end

  let (:mapping_with_mapped_key) do
    [{
      'key' => 'action_type',
      'map' => 'lambda { |k, v|
        action = v.find { |x| x["action"] == "test" }
        { "action_#{action["action"]}" => action["value"] }
      }'
    }]
  end

  let (:mapped_key_dataset) do
    [{
      'id' => 1,
      'action_type' => [{
        'action' => 'test',
        'value' => 100
      }]
    }]
  end

  let (:expected_dataset_with_mapped_key) do
    [{
      'id' => 1,
      'action_type' => [{
        'action' => 'test',
        'value' => 100
      }],
      'action_test' => 100
    }]
  end

  let (:not_enough_deps_node_node) do
    Dataflow::Nodes::MapNode.create(
      name: 'map_node',
      dependency_ids: [base_node],
      data_node_id: make_data_node('data')
    )
  end
end
