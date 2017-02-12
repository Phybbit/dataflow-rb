# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::SelectKeysNode, type: :model do
  before do
    base_node.add(records: dataset)
  end

  describe '#validate!' do
    it 'throws if there is not 1 dependency' do
      expect { not_enough_deps_node_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is no keys to be selected' do
      expect { no_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'keeps the same amount of records' do
      select_node.recompute
      records = select_node.all
      expect(records.count).to eq(expected_dataset.count)
    end

    it 'keeps only the selected keys' do
      select_node.recompute
      data = select_node.all
      expect_dataset_are_equal(data, expected_dataset, key: 'id')
    end
  end

  let(:base_node) do
    make_data_node('data1')
  end

  let(:select_node) do
    Dataflow::Nodes::SelectKeysNode.create(
      name: 'select_node',
      dependency_ids: [base_node],
      keys: %w(id value1 value2 complex|value),
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset) do
    [{
      'id' => 1,
      'value1' => '1',
      'value2' => '2',
      'value3' => '3',
      'complex' => { 'value' => 1 }
    },
     {
       'id' => 2,
       'value2' => '2',
       'value4' => '4',
       'value6' => '6'
     },
     {
       'object without any keys' => 'should not be present in final result'
     }]
  end

  let (:expected_dataset) do
    [{
      'id' => 1,
      'value1' => '1',
      'value2' => '2',
      'complex' => { 'value' => 1 }
    },
     {
       'id' => 2,
       'value2' => '2'
     }]
  end

  let (:not_enough_deps_node_node) do
    Dataflow::Nodes::SelectKeysNode.create(
      name: 'select_node',
      dependency_ids: [],
      keys: ['key1'],
      data_node_id: make_data_node('data')
    )
  end

  let (:no_key_node) do
    Dataflow::Nodes::SelectKeysNode.create(
      name: 'select_node',
      dependency_ids: [base_node],
      data_node_id: make_data_node('data')
    )
  end
end
