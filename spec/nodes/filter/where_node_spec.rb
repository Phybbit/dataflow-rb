require 'spec_helper'

RSpec.describe Dataflow::Nodes::Filter::WhereNode, type: :model do
  before do
    base_node.add(records: dataset)
  end

  describe '#validate!' do
    it 'throws if there is not 1 dependency' do
      expect { not_enough_deps_node_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is no key' do
      expect { no_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is no op key' do
      expect { no_op_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if op key is not supported' do
      expect { wrong_op_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute (op: :eq)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'eq')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_eq, key: 'id')
    end
  end

  describe '#recompute (op: :ne)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'ne')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_ne, key: 'id')
    end
  end

  describe '#recompute (op: :lt)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'lt')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_lt, key: 'id')
    end
  end

  describe '#recompute (op: :le)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'le')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_le, key: 'id')
    end
  end

  describe '#recompute (op: :gt)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'gt')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_gt, key: 'id')
    end
  end

  describe '#recompute (op: :ge)' do
    it 'keeps only the elements that match the condition' do
      node = make_node(op: 'ge')
      node.recompute
      data = node.all
      expect_dataset_are_equal(data, expected_dataset_ge, key: 'id')
    end
  end

  let (:base_node) {
    make_data_node('data')
  }

  def make_node(op:)
    Dataflow::Nodes::Filter::WhereNode.create(
      name: 'node',
      dependency_ids: [base_node],
      key: 'id',
      op: op,
      value: 2,
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset) {
    [
      { 'id' => 1 },
      { 'id' => 2 },
      { 'id' => 3 },
    ]
  }

  let (:expected_dataset_eq) { [{ 'id' => 2 }] }
  let (:expected_dataset_ne) { [{ 'id' => 1 }, { 'id' => 3 }] }
  let (:expected_dataset_lt) { [{ 'id' => 1 }] }
  let (:expected_dataset_le) { [{ 'id' => 1 }, { 'id' => 2 }] }
  let (:expected_dataset_gt) { [{ 'id' => 3 }] }
  let (:expected_dataset_ge) { [{ 'id' => 2 }, { 'id' => 3 }] }

  let (:not_enough_deps_node_node) {
    Dataflow::Nodes::Filter::WhereNode.create(
      name: 'node',
      dependency_ids: [],
      key: 'id',
      op: 'eq',
      value: '',
      data_node_id: make_data_node('data')
    )
  }

  let (:no_key_node) {
    Dataflow::Nodes::Filter::WhereNode.create(
      name: 'node',
      dependency_ids: [base_node],
      op: 'eq',
      data_node_id: make_data_node('data')
    )
  }

  let (:no_op_key_node) {
    Dataflow::Nodes::Filter::WhereNode.create(
      name: 'node',
      dependency_ids:[base_node],
      key: 'id',
      value: '',
      data_node_id: make_data_node('data')
    )
  }

  let (:wrong_op_key_node) {
    Dataflow::Nodes::Filter::WhereNode.create(
      name: 'node',
      dependency_ids:[base_node],
      key: 'id',
      op: '?',
      value: '',
      data_node_id: make_data_node('data')
    )
  }
end
