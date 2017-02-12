require 'spec_helper'

RSpec.describe(Dataflow::Nodes::SqlQueryNode, type: :model) do

  before do
    base_node.add(records: base_dataset)
    query_node.query = query
  end

  let(:query) {
    'INSERT INTO <node> (value) SELECT distinct(value) FROM <0>'
  }

  describe '#computed_query' do
    it 'parses & replace the node (write) dataset name' do
      expected_query = 'INSERT INTO query_node_buffer2 (value)'
      expect(query_node.computed_query.starts_with?(expected_query)).to eq true
    end

    it 'parses & replace the dependencies (read) dataset names' do
      expected_query = '(value) SELECT distinct(value) FROM base_node'
      expect(query_node.computed_query.ends_with?(expected_query)).to eq true
    end
  end

  context 'select distinct value from dependency' do
    it 'runs the query' do
      query_node.recompute
      expect_dataset_are_equal(query_node.all, expected_dataset, key: :value)
    end
  end

  let(:query_node) {
    out_node_params = {
      db_name: 'dataflow_test',
      name: 'query_node',
      db_backend: :postgresql,
      schema: {
        value: {type: 'integer'}
      }
    }
    params = {
      name: 'sql_query',
      data_node_id: Dataflow::Nodes::DataNode.create(out_node_params),
      dependency_ids: [base_node.id],
    }
    Dataflow::Nodes::SqlQueryNode.create(params)
  }

  let(:base_node) {
    params = {
      db_name: 'dataflow_test',
      name: 'base_node',
      db_backend: :postgresql,
      schema: {
        id: {type: 'integer', max: 100},
        name: {type: 'string', max: 255},
        value: {type: 'integer', max: 100}}
    }
    Dataflow::Nodes::DataNode.create(params)
  }

  let (:base_dataset) {
    [
      {id: 1, name: 'a', value: 1},
      {id: 2, name: 'b', value: 2},
      {id: 3, name: 'c', value: 3},
      {id: 4, name: 'd', value: 1},
      {id: 5, name: 'e', value: 2},
      {id: 6, name: 'f', value: 3},
      {id: 7, name: 'g', value: 4},
    ]
  }

  let (:expected_dataset) {
    [
      {value: 1},
      {value: 2},
      {value: 3},
      {value: 4},
    ]
  }

end
