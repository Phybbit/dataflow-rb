require 'spec_helper'

RSpec.describe Dataflow::Adapters::MongoDbAdapter, type: :model do

  context 'selection' do
    before do
      client[dataset_name].insert_many(dummy_data)
    end

    include_examples 'adapter #find'
    include_examples 'adapter #all'
    include_examples 'adapter #all_paginated'
    include_examples 'adapter #count'

    describe '#all' do
      it 'properly casts _mojaco_updated_at where values to time' do
        res = adapter.all(where: {'_mojaco_updated_at' => {'>=' => '2016-02-01'}})
        expect(res).to eq(dummy_data[2..-1])
      end
    end

    it 'returns queries for parallel processing' do
      ids = adapter.all(fields: ['_id'], sort: {'_id' => 1}).map { |x| x['_id'].to_s }
      expect(ids.size).to eq 5

      queries = adapter.ordered_system_id_queries(batch_size: 2)

      expect(queries.count).to eq 3
      expect(queries[0]).to eq({'_id' => {'>=' => ids[0], '<'  => ids[2]}})
      expect(queries[1]).to eq({'_id' => {'>=' => ids[2], '<'  => ids[4]}})
      expect(queries[2]).to eq({'_id' => {'>=' => ids[4], '<=' => ids[4]}})
    end
  end

  context 'write' do
    include_examples 'adapter #save'
    include_examples 'adapter #delete'
  end

  describe '#recreate_dataset' do
    before do
      client[dataset_name].insert_many(dummy_data)
      adapter.recreate_dataset
    end

    it 'removes any previous data' do
      record_count = client[dataset_name].find().count
      expect(record_count).to eq 0
    end

    it 'recreates the collection' do
      exists = client.collections.any? { |x| x.name == dataset_name }
      expect(exists).to eq true
    end
  end

  describe '#create_indexes' do
    it 'adds all the given indexes' do
      adapter.create_indexes
      indexes.each do |index|
        exists = client[dataset_name].indexes.any? { |mongo_index|
          mongo_index['key'].keys == Array(index[:key])
        }.present?
        expect(exists).to eq true
      end
    end

    it 'supports the unique property' do
      adapter.create_indexes
      compound_index = client[dataset_name].indexes.find { |x|
        x['key'].keys == ['id', 'updated_at']
      }
      expect(compound_index['unique']).to eq true
    end
  end

  describe '#usage' do
    it 'fetches the used memory size' do
      expect(adapter.usage(dataset: dataset_name)[:memory].present?).to eq true
    end

    it 'fetches the used storage size' do
      expect(adapter.usage(dataset: dataset_name)[:storage].present?).to eq true
    end

    it 'fetches the effective indexes' do
      adapter.create_indexes
      expected_indexes = [
        {'key' => ['id']},
        {'key' => ['updated_at']},
        {'key' => ['id', 'updated_at'], 'unique' => true}
      ]

      expect(adapter.usage(dataset: dataset_name)[:effective_indexes]).to eq(expected_indexes)
    end
  end


  let(:client) { Mongoid::Clients.default }
  let(:db_name) { 'dataflow_test' }
  let(:dataset_name) { 'test_table' }
  let(:indexes) { [
      { key: 'id' },
      { key: 'updated_at' },
      { key: ['id', 'updated_at'], unique: true }
    ] }
  let(:data_node) {
    Dataflow::Nodes::DataNode.new({
                                    db_name: db_name,
                                    name: dataset_name,
                                    indexes: indexes
    })
  }
  let(:dummy_data) {
    [
      {'id' => 1, 'updated_at' => '2016-01-01'.to_time, 'value' => 1, '_mojaco_updated_at' => Time.parse('2016-01-01') },
      {'id' => 1, 'updated_at' => '2016-01-15'.to_time, 'value' => 2, '_mojaco_updated_at' => Time.parse('2016-01-15') },
      {'id' => 1, 'updated_at' => '2016-02-02'.to_time, 'value' => 3, '_mojaco_updated_at' => Time.parse('2016-02-02') },
      {'id' => 2, 'updated_at' => '2016-02-02'.to_time, 'value' => 2, '_mojaco_updated_at' => Time.parse('2016-02-02') },
      {'id' => 3, 'updated_at' => '2016-02-02'.to_time, 'value' => 3, '_mojaco_updated_at' => Time.parse('2016-02-02') },
    ]
  }
  let(:adapter) {
    Dataflow::Adapters::MongoDbAdapter.new(data_node: data_node)
  }
end
