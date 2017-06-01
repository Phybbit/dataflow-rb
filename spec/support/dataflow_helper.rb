# frozen_string_literal: true

def make_data_node_params(name, opts = {})
  params = {
    db_name: TEST_DB_NAME,
    name: name
  }
  params.reverse_merge!(opts)
end

# Creates a Dataflow::Nodes::DataNode
def make_data_node(name, opts = {})
  Dataflow::Nodes::DataNode.create(make_data_node_params(name, opts))
end

# compare 2 datasets independently of they hashes keys order
def expect_dataset_are_equal(d1, d2, key:)
  expect(d1.count).to eq d2.count

  keys = Array(key)
  d1.each_with_index do |x, _i|
    y = d2.find { |rec| keys.all? { |key| rec[key] == x[key] } }
    expect(y).not_to be nil

    x.each do |k|
      expect(x[k]).to eq(y[k])
    end

    expect(x.keys.count).to eq(y.keys.count)
  end
end
