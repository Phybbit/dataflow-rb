require 'spec_helper'

RSpec.describe Dataflow::Nodes::UpsertNode, type: :model do
  describe '#add' do
    context 'simple key' do
      before do
        node.add(records: [{ 'unique_id' => 1, 'data' => 1 }])
        node.add(records: [{ 'unique_id' => 1, 'data' => 2 }])
      end

      it 'keeps the data unique (relative to the index)' do
        expect(node.all.count).to eq 1
      end

      it 'replaces the data' do
        expect(node.all).to eq([{ 'unique_id' => 1, 'data' => 2 }])
      end

      it 'creates a unique index' do
        unique_key = { 'key' => 'unique_id', 'unique' => true }
        expect(node.indexes.include?(unique_key)).to eq true
      end
    end

    context 'compound index node' do
      before do
        2.times do |x|
          compound_index_node.add(records: [{
            'unique_id' => 1,
            'another_key' => 1,
            'data' => x+1
          }])
        end
      end

      it 'support a compound unique index' do
        unique_key = { 'key' => ['unique_id', 'another_key'], 'unique' => true }
        expect(compound_index_node.indexes.include?(unique_key)).to eq true
      end

      it 'adds each key of the unique index as a non-unique index' do
        key1 = { 'key' => 'unique_id' }
        key2 = { 'key' => 'another_key' }
        expect(compound_index_node.indexes.include?(key1)).to eq true
        expect(compound_index_node.indexes.include?(key2)).to eq true
      end

      it 'keeps the data unique (relative to the index)' do
        expect(compound_index_node.all.count).to eq 1
      end

      it 'replaces the data' do
        expect(compound_index_node.all).to eq([{
          'unique_id' => 1,
          'another_key' => 1,
          'data' => 2
        }])
      end
    end

    context 'with indexes already set' do
      it 'does not add more (redundant) indexes' do
        expect(index_node_with_indexes.indexes.count).to eq 1
      end
    end

    context 'use mojaco timestamp' do
      it 'adds internal update at timestamps' do
        time = '2016-01-01T00:00:00Z'
        Timecop.freeze(time) do
          node.use_internal_timestamp = true
          node.add(records: [{'id' => 1}])
          expect(node.all).to eq([{'id' => 1, '_mojaco_updated_at' => time.to_time}])
        end
      end
    end
  end

  describe 'before save callback' do
    it 'tranforms index key into arrays' do
      node.index_key = 'unique,key'
      node.save
      expect(node.index_key).to eq ['unique', 'key']
    end

    it 'keeps index key as string if there is only a single value' do
      node.index_key = 'unique'
      node.save
      expect(node.index_key).to eq 'unique'
    end

    it 'still works without index key' do
      node.index_key = nil
      node.save
      expect(node.index_key).to eq nil
    end
  end

  describe 'error handling' do
    it 'raises an argument error if the records are not an array' do
      expect{ node.add(records: 'not an array') }.to raise_error(ArgumentError)
    end

    it 'handles nil in the records' do
      node.add(records: [{'id' => 1}, nil])
      expect(node.all).to eq([{'id' => 1}])
    end
  end

  let (:node) {
    params = make_data_node_params('raw_data_unique',
      index_key: 'unique_id',
      use_internal_timestamp: false,
    )
    Dataflow::Nodes::UpsertNode.create(params)
  }

  let (:compound_index_node) {
    params = make_data_node_params('unique_compound_key',
      index_key: ['unique_id', 'another_key'],
      use_internal_timestamp: false
    )
    Dataflow::Nodes::UpsertNode.create(params)
  }

  let (:index_node_with_indexes) {
    params = make_data_node_params('unique_compound_key',
      index_key: 'unique_id',
      indexes: [{
        'key' => 'unique_id',
        'unique' => true
      }]
    )
    Dataflow::Nodes::UpsertNode.create(params)
  }

  let (:index_node_without_index_key) {
    params = make_data_node_params('unique_compound_key',
      indexes: [{
        'key' => 'unique_id',
        'unique' => true
      }]
    )
    Dataflow::Nodes::UpsertNode.create(params)
  }
end
