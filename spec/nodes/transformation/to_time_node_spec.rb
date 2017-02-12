# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::Transformation::ToTimeNode, type: :model do
  before do
    base_node.add(records: dataset)
  end

  describe '#validate!' do
    it 'throws if there is not 1 dependency' do
      expect { not_enough_deps_node_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is keys' do
      expect { no_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'keeps the same amount of records' do
      transform_node.recompute
      records = transform_node.all
      expect(records.count).to eq(expected_dataset.count)
    end

    it 'only transforms the specified keys' do
      transform_node.recompute
      data = transform_node.all
      expect_dataset_are_equal(data, expected_dataset, key: 'id')
    end
  end

  let (:base_node) do
    make_data_node('data1')
  end

  let (:transform_node) do
    Dataflow::Nodes::Transformation::ToTimeNode.create(
      name: 'transform_node',
      dependency_ids: [base_node],
      keys: %w(start_time end_time),
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset) do
    [{
      'id' => 1,
      'value1' => 'not touched',
      'start_time' => '2016-01-01T15:00:00Z',
      'end_time' => '2016-01-02T15:00:00Z'
    },
     {
       'id' => 2,
       'value2' => 'no time key is fine as well'
     },
     {
       'id' => 3,
       'start_time' => 'invalid date',
       'value2' => 'invalid date is nil'
     }]
  end

  let (:expected_dataset) do
    [{
      'id' => 1,
      'value1' => 'not touched',
      'start_time' => '2016-01-01T15:00:00Z'.to_time,
      'end_time' => '2016-01-02T15:00:00Z'.to_time
    },
     {
       'id' => 2,
       'value2' => 'no time key is fine as well'
     },
     {
       'id' => 3,
       'start_time' => nil,
       'value2' => 'invalid date is nil'
     }]
  end

  let (:not_enough_deps_node_node) do
    Dataflow::Nodes::Transformation::ToTimeNode.create(
      name: transform_node,
      dependency_ids: [],
      keys: ['key1'],
      data_node_id: make_data_node('data')
    )
  end

  let (:no_key_node) do
    Dataflow::Nodes::Transformation::ToTimeNode.create(
      name: 'transform_node',
      dependency_ids: [base_node],
      keys: nil,
      data_node_id: make_data_node('data')
    )
  end
end
