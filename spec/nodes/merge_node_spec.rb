# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::MergeNode, type: :model do
  before do
    node1.add(records: dataset1)
    node2.add(records: dataset2)
  end

  describe '#validate!' do
    it 'throws if there is not 2 dependencies' do
      expect { not_enough_deps_node_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'merges both datasets' do
      merge_node.recompute
      expect_dataset_are_equal(merge_node.all, dataset1 + dataset2, key: 'id')
    end

    it 'adds the merge key with corresponding values' do
      merge_node.merge_key = 'origin'
      merge_node.merge_values = %w(dataset1 dataset2)

      merge_node.recompute
      expect_dataset_are_equal(merge_node.all, expected_dataset_with_merge_key, key: 'id')
    end
  end

  let (:node1) do
    make_data_node('data1')
  end
  let (:node2) do
    make_data_node('data2')
  end
  let (:merge_node) do
    Dataflow::Nodes::MergeNode.create(
      name: 'merge_node',
      dependency_ids: [node1, node2],
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset1) do
    [{
      'id' => 1,
      'value' => 'value1'
    }]
  end

  let (:dataset2) do
    [{
      'id' => 1,
      'value' => 'value2'
    }]
  end

  let (:expected_dataset_with_merge_key) do
    [{
      'id' => 1,
      'value' => 'value1',
      'origin' => 'dataset1'
    },
     {
       'id' => 1,
       'value' => 'value2',
       'origin' => 'dataset2'
     }]
  end

  let (:not_enough_deps_node_node) do
    Dataflow::Nodes::MergeNode.create(
      name: 'merge_node',
      dependency_ids: [node1],
      data_node_id: make_data_node('data')
    )
  end
end
