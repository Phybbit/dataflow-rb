require 'spec_helper'

RSpec.describe Dataflow::Adapters::MysqlAdapter, type: :model do

  def create_test_dataset
    client.create_table dataset_name do
      primary_key :_id
      Integer :id
      DateTime :updated_at
      Integer :value
      String :value_s
    end
  end

  context 'selection' do
    before do
      create_test_dataset
      dummy_data.each { |d| client[dataset_name.to_sym].insert(d) }
    end

    describe 'initialization' do
      it 'creates a DB if it does not exist' do
        client.run("DROP DATABASE dataflow_test")
        adapter

        expect { client.run("USE dataflow_test") }.not_to raise_error
      end
    end

    include_examples 'adapter #find',  use_sym: true
    include_examples 'adapter #all',   use_sym: true, adapter_type: 'mysql'
    include_examples 'adapter #count', use_sym: true
  end

  context 'write' do
    before do
      create_test_dataset
    end

    include_examples 'adapter #save', use_sym: true
    include_examples 'adapter #delete', use_sym: true
  end

  describe '.disconnect_clients' do
    it 'supports disconnecting clients' do
      adapter.client.test_connection
      expect(adapter.client.pool.available_connections.count).to eq 1

      Dataflow::Adapters::SqlAdapter.disconnect_clients
      expect(adapter.client.pool.available_connections.count).to eq 0
    end
  end


  describe '#usage' do
    before do
      create_test_dataset
      dummy_data.each { |d| client[dataset_name.to_sym].insert(d) }
    end

    it 'fetches the used memory size' do
      expect(adapter.usage(dataset: dataset_name)[:memory]).to be > 0
    end

    it 'fetches the used storage size' do
      expect(adapter.usage(dataset: dataset_name)[:storage]).to be > 0
    end

    it 'fetches the effective indexes' do
      adapter.create_indexes
      expected_indexes = [
        {'key' => ['id']},
        {'key' => ['updated_at']},
        {'key' => ['id', 'updated_at'], 'unique' => true}
      ]
      effective_indexes = adapter.usage(dataset: dataset_name)[:effective_indexes]

      expect(effective_indexes - expected_indexes).to eq([])
      expect(expected_indexes - effective_indexes).to eq([])
    end
  end

  let(:client) { MysqlTestClient } # TODO: make tests for Mysql and PostgreSQL
  let(:db_name) { 'dataflow_test' }
  let(:dataset_name) { 'test_table' }
  let(:indexes) { [
      { 'key' => 'id' },
      { 'key' => 'updated_at' },
      { 'key' => ['id', 'updated_at'], 'unique' => true }
    ]
  }
  let(:dummy_data) {
    [
      { id: 1, updated_at: '2016-01-01'.to_time, value: 1, value_s: 'aaa'},
      { id: 1, updated_at: '2016-01-15'.to_time, value: 2, value_s: 'AAA'},
      { id: 1, updated_at: '2016-02-02'.to_time, value: 3, value_s: 'bbb'},
      { id: 2, updated_at: '2016-02-02'.to_time, value: 2, value_s: '011'},
      { id: 3, updated_at: '2016-02-02'.to_time, value: 3, value_s: '012'},
    ]
  }
  let(:data_node) {
    Dataflow::Nodes::DataNode.new({db_name: db_name,
                                   name: dataset_name,
                                   indexes: indexes})
  }
  let(:adapter) {
    Dataflow::Adapters::MysqlAdapter.new(data_node: data_node, adapter_type: 'mysql')
  }
end
