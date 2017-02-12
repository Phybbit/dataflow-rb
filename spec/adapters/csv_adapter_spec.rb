require 'spec_helper'

RSpec.describe Dataflow::Adapters::CsvAdapter, type: :model do
  before do
    data_node
  end

  def make_schema(keys)
    keys.map { |k| [k, {type: 'string'}] }.to_h
  end

  describe '#save' do
    it 'adds multiple data to the file' do
      adapter.set_schema(make_schema(['id', 'updated_at', 'value']))
      adapter.save(records: simple_data)
      adapter.on_save_finished

      lines = parse_test_csv
      expected_lines = [
        ['id','updated_at','value'],
        ['1','2016-01-01','1'],
        ['2','2016-01-02','2']
      ]
      expect(lines).to eq expected_lines
    end

    it 'supports hash data' do
      adapter.set_schema(make_schema(['id', 'updated_at', 'complex_value|s']))
      adapter.save(records: hash_data)
      adapter.on_save_finished

      lines = parse_test_csv
      expected_lines = [
        ['id','updated_at','complex_value|s'],
        ['1','2016-01-01','1'],
      ]
      expect(lines).to eq expected_lines
    end

    it 'supports array data' do
      adapter.set_schema(make_schema(['id', 'updated_at', 'array_values|0','array_values|1']))
      adapter.save(records: array_data)
      adapter.on_save_finished

      lines = parse_test_csv
      expected_lines = [
        ['id','updated_at','array_values|0','array_values|1'],
        ['1','2016-01-01','1','2'],
      ]
      expect(lines).to eq expected_lines
    end

    it 'supports hashes in array data' do
      adapter.set_schema(make_schema(['id', 'updated_at', 'array_values|0|s']))
      adapter.save(records: hash_in_array_data)
      adapter.on_save_finished

      lines = parse_test_csv

      expected_lines = [
        ['id','updated_at','array_values|0|s'],
        ['1','2016-01-01','1'],
      ]
      expect(lines).to eq expected_lines
    end

    it 'supports arrays in hash data' do
      adapter.set_schema(make_schema(['id', 'updated_at', 'complex_value|s|0']))
      adapter.save(records: array_in_hash_data)
      adapter.on_save_finished

      lines = parse_test_csv
      expected_lines = [
        ['id','updated_at','complex_value|s|0'],
        ['1','2016-01-01','1'],
      ]
      expect(lines).to eq expected_lines
    end
  end

  describe '#all' do
    it 'reads from the csv and return an hash' do
      File.write(filepath, "id,value,updated_at\n1,2,2016-01-01")
      result = adapter.all
      expect(result).to eq([{'id' => 1, 'value' => 2, 'updated_at' => '2016-01-01'}])
    end
  end

  describe '#count' do
    it 'reads from the csv and return the entries count' do
      File.write(filepath, "id,value,updated_at\n1,2,2016-01-01")
      count = adapter.count
      expect(count).to eq 1
    end
  end


  let(:db_name) { 'dataflow_test' }
  let(:dataset_name) { 'test_table' }
  let(:updated_at) { '2016-01-01T00:00:00+09:00' }
  let(:filepath) { "#{Dataflow::CsvPath}/#{db_name}.#{dataset_name}.csv" }

  def parse_test_csv
    CSV.read(filepath)
  end

  let(:data_node) {
    Dataflow::Nodes::DataNode.create({
                                       db_name: db_name,
                                       name: dataset_name,
                                       updated_at: updated_at,
                                       db_backend: :csv
    })
  }
  let(:simple_data) {
    [
      {'id' => 1, 'updated_at' => '2016-01-01', 'value' => 1},
      {'id' => 2, 'updated_at' => '2016-01-02', 'value' => 2},
    ]
  }
  let(:hash_data) {
    [
      {
        'id' => 1,
        'updated_at' => '2016-01-01',
        'complex_value' => {'s' => 1},
      },
    ]
  }
  let(:array_data) {
    [
      {
        'id' => 1,
        'updated_at' => '2016-01-01',
        'array_values' => [1,2]
      },
    ]
  }
  let(:hash_in_array_data) {
    [
      {
        'id' => 1,
        'updated_at' => '2016-01-01',
        'array_values' => [{'s' => 1}]
      },
    ]
  }
  let(:array_in_hash_data) {
    [
      {
        'id' => 1,
        'updated_at' => '2016-01-01',
        'complex_value' => {'s' => [1] }
      },
    ]
  }
  let(:adapter) {
    Dataflow::Adapters::CsvAdapter.new(data_node: data_node)
  }
end
