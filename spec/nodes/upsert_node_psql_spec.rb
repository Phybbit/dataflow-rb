# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::UpsertNode, type: :model do
  describe '#add' do
    context 'simple key' do
      before do
        node.add(records: [{ unique_id: 1, data: 1 }])
        node.add(records: [{ unique_id: 1, data: 2 }])
      end

      it 'keeps the data unique (relative to the index)' do
        expect(node.all.count).to eq 1
      end

      it 'replaces the data' do
        expect(node.all).to eq([{ unique_id: 1, data: 2 }])
      end
    end
  end

  context 'compound index node' do
    before do
      2.times do |x|
        compound_index_node.add(records: [{
                                  unique_id: 1,
                                  another_key: 1,
                                  data: x + 1
                                }])
      end
    end

    it 'support a compound unique index' do
      unique_key = { 'key' => %w(unique_id another_key), 'unique' => true }
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
                                              unique_id: 1,
                                              another_key: 1,
                                              data: 2
                                            }])
    end
  end

  context 'with indexes already set' do
    it 'does not add more (redundant) indexes' do
      expect(index_node_with_indexes.indexes.count).to eq 1
    end
  end


  let(:node) do
    params = make_data_node_params('raw_data_unique',
                                   index_key: 'unique_id',
                                   use_internal_timestamp: false,
                                   db_backend: :postgresql,
                                   schema: {
                                     'unique_id' => { type: 'integer' },
                                     'data' =>      { type: 'integer' }
                                   })
    Dataflow::Nodes::UpsertNode.create(params)
  end

  let(:compound_index_node) do
    params = make_data_node_params('unique_compound_key',
                                   index_key: %w(unique_id another_key),
                                   use_internal_timestamp: false,
                                   db_backend: :postgresql,
                                   schema: {
                                     'unique_id' =>   { type: 'integer' },
                                     'another_key' => { type: 'integer' },
                                     'data' =>        { type: 'integer' }
                                   })
    Dataflow::Nodes::UpsertNode.create(params)
  end

  let(:index_node_with_indexes) do
    params = make_data_node_params('unique_compound_key',
                                   index_key: 'unique_id',
                                   indexes: [{
                                     'key' => 'unique_id',
                                     'unique' => true
                                   }])
    Dataflow::Nodes::UpsertNode.create(params)
  end

end
