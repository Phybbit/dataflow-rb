# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::DataNode, type: :model do
  describe '#import' do
    let (:data) do
      [{ 'a' => 1, 'b' => 'test' }]
    end

    it 'imports from mysql' do
      # TODO: re-write with a simple .add when we support it
      keys = data[0].keys.join(',')
      MysqlTestClient.create_table :node do
        Integer :a
        String :b
      end

      data.each { |d| MysqlTestClient[:node].insert(d) }

      node.import(connection_opts: { db_backend: :mysql, connection_uri: 'mysql2://root@localhost', db_name: 'dataflow_test', dataset_name: 'node' })
      expect(node.all).to eq(data)
    end

    it 'imports from csv' do
      # TODO: fix this interface access
      adapter = csv_node.send(:db_adapter)
      adapter.set_schema('a' => 'string', 'b' => 'string')
      adapter.save(records: data)
      adapter.on_save_finished

      node.import(connection_opts: { db_backend: :csv })
      expect(node.all).to eq(data)
    end
  end

  context 'double buffering' do
    before do
      node.use_double_buffering = true
    end

    it 'has different read/write dataset names' do
      expect(node.read_dataset_name).not_to eq(node.write_dataset_name)
    end

    describe '#read_dataset_name=' do
      it 'supports setting the read_dataset_name' do
        node.read_dataset_name = node.write_dataset_name
        expect(node.read_dataset_name).to eq(node.write_dataset_name)
      end

      it 'does not let set the read dataset name to anything else other than the available buffers' do
        name = node.read_dataset_name
        node.read_dataset_name = 'whatever'
        expect(node.read_dataset_name).to eq(name)
      end
    end
  end

  describe '#export' do
    let (:data) do
      [{ 'a' => 1, 'b' => 'test', 'deep' => { 'nested' => 'value', 'array' => [0,1,2] }}]
    end

    before do
      node.add(records: data)
    end

    it 'exports to a different backend' do
      node.export(connection_opts: { db_backend: :csv })
      expect(csv_node.all).to eq([{'a' => 1, 'b' => 'test', 'deep|nested' => 'value', 'deep|array|0' => 0, 'deep|array|1' => 1, 'deep|array|2' => 2}])
    end

    it 'calls the export_started evt' do
      called = 0
      node.export_started { called += 1 }
      node.export(connection_opts: { db_backend: :csv })
      expect(called).to eq 1
    end

    it 'calls the export_finished evt' do
      called = 0
      node.export_finished { called += 1 }
      node.export(connection_opts: { db_backend: :csv })
      expect(called).to eq 1
    end
  end

  describe '#sample_data' do
    let (:data) do
      [
        { 'simple'  => 'value' },
        { 'complex' => { 'data' => 'type' } },
        { 'arrays'  => [0, 1, 2] },
        { 'bool'    => false }
      ]
    end
    let (:tabular_data) do
      [
        { 'simple' => 'value' },
        { 'complex|data' => 'type' },
        { 'arrays|0' => 0, 'arrays|1' => 1, 'arrays|2' => 2 },
        { 'bool' => false }
      ]
    end

    it 'throws if we pass an unsupported mode' do
      expect { node.sample_data(mode: :unknown) }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'controls the amount of samples returned' do
      node.add(records: data)
      samples = node.sample_data(count: 1)
      expect(samples.count).to eq 1
    end

    context 'raw output' do
      it 'outputs sample data' do
        node.add(records: data)
        samples = node.sample_data(count: 4, mode: :raw)
        expect(samples).to eq data
      end
    end

    context 'tabular output' do
      it 'outputs sample data with flat keys' do
        node.add(records: data)
        samples = node.sample_data(count: 4, mode: :tabular)
        expect(samples).to eq tabular_data
      end
    end
  end

  describe '#infer_schema' do
    before do
      data = [
        { 'a' => 1, 'b' => 'string', 'c' => [1, { 'test' => 'a' }], 'd' => { 'hash' => 'a', 'array_test' => [1, 'a'] } },
        { 'a' => 'became mixed', 'c' => [2], 'd' => { 'hash' => 'a' }, 'e' => 'new field!' },
        { 'f' => true, 'g' => 1, 'h' => 1.0, 'gs' => '1', 'hs' => '1.0' },
        { 'array' => [0, 1, nil, 2] },
        { 'array' => [4] },
        { 'with_nulls' => nil },
        { 'with_nulls' => [4] }
      ]
      node.add(records: data)
    end

    it 'recognizes numbers' do
      expect(node.infer_schema['h'][:type]).to eq 'numeric'
      expect(node.infer_schema['hs'][:type]).to eq 'numeric'
    end

    it 'recognizes integers' do
      expect(node.infer_schema['g'][:type]).to eq 'integer'
      expect(node.infer_schema['gs'][:type]).to eq 'integer'
    end

    it 'recognizes booleans' do
      expect(node.infer_schema['f'][:type]).to eq 'boolean'
    end

    it 'recognizes strings' do
      expect(node.infer_schema['b'][:type]).to eq 'string'
    end

    it 'recognizes arrays' do
      expect(node.infer_schema['b'][:type]).to eq 'string'
    end

    it 'recognizes fields that change type' do
      expect(node.infer_schema['a'][:type]).to eq 'object'
    end

    it 'recognizes (terminal) arrays' do
      expect(node.infer_schema['array'][:type]).to eq 'array'
      expect(node.infer_schema['array'][:usage_count]).to eq 2
      expect(node.infer_schema['array'][:min_size]).to eq 1
      expect(node.infer_schema['array'][:max_size]).to eq 4
    end

    it 'recognizes fields in arrays' do
      expect(node.infer_schema['c|1|test'][:type]).to eq 'string'
    end

    it 'removes null keys that have full keys with other objects' do
      expect(node.infer_schema['with_nulls'][:type]).not_to eq 'nilclass'
      expect(node.infer_schema['with_nulls'][:usage_count]).to eq 1
    end

    it 'recognizes fields in hashes' do
      expect(node.infer_schema['d|hash'][:type]).to eq 'string'
      expect(node.infer_schema['d|array_test'][:type]).to eq 'array'
    end

    it 'sets the inferred at' do
      node.infer_schema
      expect(node.inferred_schema_at.class).to eq Time
    end

    it 'supports inferring schema from a limited amount of samples' do
      sch = node.infer_schema(samples_count: 1)
      expect(node['with_nulls']).to be nil
    end

    it 'sets the inferred from' do
      node.infer_schema(samples_count: 1)
      expect(node.inferred_schema_from).to eq 1
    end

    it 'saves the inferred schema' do
      node.infer_schema
      node.reload
      expect(node.inferred_schema).not_to be nil
    end

    it 'triggers a started evt' do
      called = 0
      node.schema_inference_started { called += 1 }
      node.infer_schema
      expect(called).to eq 1
    end

    it 'triggers a finished evt' do
      called = 0
      node.schema_inference_finished { called += 1 }
      node.infer_schema
      expect(called).to eq 1
    end
  end

  describe 'error handling' do
    it 'raises an argument error if the records are not an array' do
      expect { node.add(records: 'not an array') }.to raise_error(ArgumentError)
    end

    it 'handles nil in the records' do
      node.add(records: [{ 'id' => 1 }, nil])
      expect(node.all).to eq([{ 'id' => 1 }])
    end
  end

  describe 'validations' do
    it 'needs the db name and ame to be set' do
      res = Dataflow::Nodes::DataNode.create
      expect(res.errors[:db_name].present?).to eq true
      expect(res.errors[:name].present?).to eq true
    end
  end

  describe '.properties' do
    it 'has properties with custom params' do
      prop = Dataflow::Nodes::DataNode.properties[:db_name]
      expect(prop).not_to be nil
      expect(prop[:editable]).to be false
    end

    it 'does not include fields with a _ prefix' do
      props = Dataflow::Nodes::DataNode.properties.keys.grep /^_/
      expect(props.blank?).to be true
    end

    it 'supports default values' do
      expect(node.db_backend).to eq(:mongodb)
    end
  end

  describe '#dump_dataset, #restore' do
    it 'dumps and restore' do
      begin
        node.add(records: [{ 'id' => 1 }])
        filepath = node.dump_dataset
        node.clear
        expect(node.count).to eq(0)

        node.restore_dataset(filepath: filepath)
        expect(node.all).to eq([{ 'id' => 1 }])
      ensure
        File.delete(filepath)
      end
    end

    it 'exports with the dataset idx to 0 for single buffer' do
      begin
        filepath = node.dump_dataset
        expect(filepath).to eq("./dump/#{node.db_name}/#{node.name}.0.gz")
      ensure
        File.delete(filepath)
      end
    end

    it 'exports with the dataset idx to 1 or 2 for double buffer' do
      begin
        node.use_double_buffering = true
        filepath = node.dump_dataset
        expect(filepath).to eq("./dump/#{node.db_name}/#{node.name}.1.gz")
      ensure
        File.delete(filepath)
      end
    end

    it 'raises if trying to restore with incompatible settings (node is a single buffered node)' do
      expect { node.restore_dataset(filepath: 'test.1.gz') }.to raise_error(RuntimeError)
    end

    it 'raises if trying to restore with incompatible settings (node is a double buffered node)' do
      node.use_double_buffering = true
      expect { node.restore_dataset(filepath: 'test.0.gz') }.to raise_error(RuntimeError)
    end
  end

  describe '#metadata' do
    it 'formats the metadata as a hash' do
      metadata = node.metadata
      expect(metadata[:_id]).to eq(node._id)
      expect(metadata[:_type]).to eq(node._type)
      expect(metadata[:name]).to eq(node.name)
      expect(metadata[:updated_at]).to eq(node.updated_at)
      expect(metadata[:db_backend]).to eq(node.db_backend)
      expect(metadata[:use_double_buffering]).to eq(node.use_double_buffering)
    end
  end

  describe '#ordered_system_id_queries' do
    before do
      node.add(records: [{id: 1, value: 1}, {id: 2, value: 2}])
    end

    it 'returns system id queries' do
      expect(node.ordered_system_id_queries(batch_size: 1).count).to eq(2)
    end

    it 'supports the filtering with a query' do
      expect(node.ordered_system_id_queries(batch_size: 1, where: {value: 1}).count).to eq(1)
    end
  end

  let(:node) { make_data_node('node') }

  let(:csv_node) do
    # make sure the original node is create
    node
    # re-instanciate another one and change its db_backend
    n = Dataflow::Nodes::DataNode.find_by(name: 'node')
    n.db_backend = :csv
    n
  end
end
