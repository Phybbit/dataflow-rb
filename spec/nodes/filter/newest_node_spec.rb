# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::Filter::NewestNode, type: :model do
  before do
    base_node.add(records: dataset)
  end

  describe '#validate!' do
    it 'throws if there is not 1 dependency' do
      expect { not_enough_deps_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is no unique key to perform the filter' do
      expect { no_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'throws if there is no date key to perform the filter' do
      expect { no_date_key_node.validate! }
        .to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end
  end

  describe '#recompute' do
    it 'keeps only the newest entries for the given ids' do
      newest_node.recompute
      data = newest_node.all
      expect_dataset_are_equal(data, expected_dataset, key: 'id')
    end
  end

  let (:base_node) do
    make_data_node('data')
  end
  let (:newest_node) do
    Dataflow::Nodes::Filter::NewestNode.create(
      name: 'filter_newest_node',
      dependency_ids: [base_node],
      id_key: 'id',
      date_key: 'updated_at',
      data_node_id: make_data_node('data')
    )
  end

  let (:dataset) do
    [{
      'id' => 1,
      'value' => 1,
      'updated_at' => '2000-01-01'
    },
     {
       'id' => 1,
       'value' => 2,
       'updated_at' => '2016-01-01'
     },
     {
       'id' => 2,
       'value' => 2,
       'updated_at' => '2016-01-01'
     }]
  end

  let (:expected_dataset) do
    [{
      'id' => 1,
      'value' => 2,
      'updated_at' => '2016-01-01'
    },
     {
       'id' => 2,
       'value' => 2,
       'updated_at' => '2016-01-01'
     }]
  end

  let (:not_enough_deps_node) do
    Dataflow::Nodes::Filter::NewestNode.create(
      name: 'filter_newest_node',
      dependency_ids: [],
      id_key: 'id',
      date_key: 'updated_at',
      data_node_id: make_data_node('data')
    )
  end

  let (:no_key_node) do
    Dataflow::Nodes::Filter::NewestNode.create(
      name: 'filter_newest_node',
      dependency_ids: [base_node],
      date_key: 'updated_at',
      data_node_id: make_data_node('data')
    )
  end

  let (:no_date_key_node) do
    Dataflow::Nodes::Filter::NewestNode.create(
      name: 'filter_newest_node',
      dependency_ids: [base_node],
      id_key: 'id',
      data_node_id: make_data_node('data')
    )
  end
end
