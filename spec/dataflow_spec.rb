require 'spec_helper'

describe Dataflow do
  it 'has a version number' do
    expect(Dataflow::VERSION).not_to be nil
  end

  it 'exports/imports a dataflow' do
    begin
      # make a dataflow
      # dataset1 > compute1 > dataset2
      d1 = Dataflow::Nodes::UpsertNode.create(
        name: 'dataset1',
        db_name: TEST_DB_NAME,
        index_key: 'id'
      )
      d1.add(records: [{'id' => 1, 'test' => 'data'}])
      d1_data = d1.all

      c1 = Dataflow::Nodes::ComputeNode.create(
        name: 'compute1',
        dependency_ids: [d1.id],
        data_node_id: make_data_node('dataset2')
      )

      # export
      archive_path = Dataflow.export(nodes: [c1], include_data: true)


      # mess up a bit the data
      d1.clear
      d1.delete
      d2 = c1.data_node
      c1.data_node_id = nil
      c1.dependency_ids = []
      c1.save

      # import the same data
      Dataflow.import(archive_path: archive_path)


      # check we got the same flow
      dataset1 = Dataflow.data_node('dataset1')
      compute1 = Dataflow.compute_node('compute1')

      expect(dataset1._id).to eq(d1._id)
      expect(dataset1.class).to eq(Dataflow::Nodes::UpsertNode)
      expect(dataset1.all).to eq(d1_data)

      expect(compute1.data_node).to eq(d2)
      expect(compute1.dependencies).to eq([dataset1])
    ensure
    `rm #{archive_path}` if archive_path
    end
  end
end
