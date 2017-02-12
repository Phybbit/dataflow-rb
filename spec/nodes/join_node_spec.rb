# frozen_string_literal: true
require 'spec_helper'

RSpec.describe(Dataflow::Nodes::JoinNode, type: :model) do
  before do
    node1.add(records: dataset1)
    node2.add(records: dataset2)
  end

  describe '#validate!' do
    it 'throws if there is no join key' do
      expect { no_key_node.compute }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is not 2 dependencies' do
      expect { not_enough_deps_node_node.compute }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'joins the 2 datasets: keys that are not joined are not kept' do
      join_node.recompute
      records = join_node.all
      expect(records.count).to eq 2
    end

    it 'joins by keys' do
      join_node.recompute
      data = join_node.all
      expect_dataset_are_equal(data, expected_dataset_no_prefix, key: 'id')
    end

    it 'supports prefix' do
      join_node.prefix1 = 'd1_'
      join_node.prefix2 = 'd2_'
      join_node.recompute
      data = join_node.all

      expect_dataset_are_equal(data, expected_dataset_with_prefix, key: 'd1_id')
    end

    it 'supports complex keys' do
      complex_key_join_node.recompute
      data = complex_key_join_node.all
      expect_dataset_are_equal(data, expected_dataset_no_prefix, key: 'id')
    end

    it 'supports matching on multiple keys' do
      node1.clear
      node2.clear
      node1.add(records: dataset_multiple_keys1)
      node2.add(records: dataset_multiple_keys2)
      multiple_key_join_node.recompute
      data = multiple_key_join_node.all

      expect_dataset_are_equal(data, expected_dataset_with_multiple_keys, key: ['id', 'di', 'complex|0|id'])
    end

    it 'supports left-joins' do
      join_node.join_type = 'left'
      join_node.recompute
      data = join_node.all
      expect_dataset_are_equal(data, expected_dataset_left_join, key: 'id')
    end
  end

  let (:node1) do
    make_data_node('data1')
  end
  let (:node2) do
    make_data_node('data2')
  end
  let (:join_node) do
    Dataflow::Nodes::JoinNode.create(
      name: 'join_node',
      dependency_ids: [node1, node2],
      key1: 'd2_id',
      key2: 'id2',
      data_node_id: make_data_node('data')
    )
  end

  let (:complex_key_join_node) do
    Dataflow::Nodes::JoinNode.create(
      name: 'join_node',
      dependency_ids: [node1, node2],
      key1: 'complex|0|id',
      key2: 'id2',
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset1) do
    [{
      'id' => 1,
      'd2_id' => 10,
      'complex' => [{ 'id' => 10 }],
      'value' => '1_value_from_d1'
    },
     {
       'id' => 2,
       'd2_id' => 12,
       'complex' => [{ 'id' => 12 }],
       'value' => '2_value_from_d1'
     },
     {
       'id' => 3,
       'd2_id' => 999,
       'value' => 'value_without_any_match_in_d2'
     }]
  end

  let (:dataset2) do
    [{
      'id2' => 10,
      'value' => '10_value_from_d2'
    },
     {
       'id2' => 12,
       'value' => '12_value_from_d2'
     },
     {
       'id2' => 888,
       'value' => 'value_without_any_match_in_d1'
     }]
  end

  let (:expected_dataset_no_prefix) do
    [{
      'id' => 1,
      'd2_id' => 10,
      'complex' => [{ 'id' => 10 }],
      'id2' => 10,
      'value' => '1_value_from_d1'
    },
     {
       'id' => 2,
       'd2_id' => 12,
       'complex' => [{ 'id' => 12 }],
       'id2' => 12,
       'value' => '2_value_from_d1'
     }]
  end

  let (:expected_dataset_left_join) do
    [{
      'id' => 1,
      'd2_id' => 10,
      'complex' => [{ 'id' => 10 }],
      'id2' => 10,
      'value' => '1_value_from_d1'
    },
     {
       'id' => 2,
       'd2_id' => 12,
       'complex' => [{ 'id' => 12 }],
       'id2' => 12,
       'value' => '2_value_from_d1'
     },
     {
       'id' => 3,
       'd2_id' => 999,
       'value' => 'value_without_any_match_in_d2'
     }]
  end

  let (:expected_dataset_with_prefix) do
    [{
      'd1_id' => 1,
      'd1_d2_id' => 10,
      'd1_complex' => [{ 'id' => 10 }],
      'd1_value' => '1_value_from_d1',
      'd2_id2' => 10,
      'd2_value' => '10_value_from_d2'
    },
     {
       'd1_id' => 2,
       'd1_d2_id' => 12,
       'd1_complex' => [{ 'id' => 10 }],
       'd1_value' => '2_value_from_d1',
       'd2_id2' => 12,
       'd2_value' => '12_value_from_d2'
     }]
  end

  let (:multiple_key_join_node) do
    Dataflow::Nodes::JoinNode.create(
      name: 'join_node',
      dependency_ids: [node1, node2],
      key1: 'id',
      key2: 'id2',
      other_keys1: ['complex1|0|id', 'di'],
      other_keys2: ['complex2|0|id2', 'di'],
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset_multiple_keys1) do
    [
      {
        'id' => 1,
        'di' => 1,
        'complex1' => [{ 'id' => 10 }],
        'value' => '1_value_from_d1'
      },
      {
        'id' => 1,
        'di' => 2,
        'complex1' => [{ 'id' => 12 }]
      },
      {
        'id' => 3,
        'di' => 3,
        'complex1' => [{ 'id' => 12 }]
      }
    ]
  end

  let (:dataset_multiple_keys2) do
    [
      {
        'id2' => 1,
        'di' => 2,
        'complex2' => [{ 'id2' => 12 }],
        'value' => 'matched'
      },
      {
        'id2' => 1,
        'di' => 2,
        'complex2' => [{ 'id2' => 10 }],
        'value' => 'not matched'
      },
      {
        'id2' => 1,
        'di' => 3,
        'complex2' => [{ 'id2' => 11 }],
        'value' => 'not matched'
      },
      {
        'id2' => 2,
        'di' => 1,
        'value' => 'not matched'
      }
    ]
  end

  let (:expected_dataset_with_multiple_keys) do
    [
      {
        'id' => 1,
        'id2' => 1,
        'di' => 2,
        'complex1' => [{ 'id' => 12 }],
        'complex2' => [{ 'id2' => 12 }],
        'value' => 'matched'
      }
    ]
  end

  let (:no_key_node) do
    Dataflow::Nodes::JoinNode.create(
      name: 'join_node',
      dependency_ids: [node1, node2],
      data_node_id: make_data_node('data')
    )
  end

  let (:not_enough_deps_node_node) do
    Dataflow::Nodes::JoinNode.create(
      name: 'join_node',
      dependency_ids: [node1],
      key1: 'id', key2: 'id2',
      data_node_id: make_data_node('data')
    )
  end
end
