require 'spec_helper'

RSpec.describe Dataflow::Nodes::ReadOnlyDataNode, type: :model do
  describe '#add' do
    it 'raises an error' do
      expect { node.add(records: []) }.to raise_error(NotImplementedError)
    end
  end

  describe '#clear' do
    it 'raises an error' do
      expect { node.clear }.to raise_error(NotImplementedError)
    end
  end

  describe '#recreate_dataset' do
    it 'raises an error' do
      expect { node.recreate_dataset }.to raise_error(NotImplementedError)
    end
  end

  describe '#create_unique_indexes' do
    it 'raises an error' do
      expect { node.create_unique_indexes }.to raise_error(NotImplementedError)
    end
  end

  describe '#create_non_unique_indexes' do
    it 'raises an error' do
      expect { node.create_non_unique_indexes }.to raise_error(NotImplementedError)
    end
  end

  describe '#read_dataset_name=' do
    it 'raises an error' do
      expect { node.read_dataset_name = "test" }.to raise_error(NotImplementedError)
    end
  end

  describe '#swap_read_write_datasets!' do
    it 'raises an error' do
      expect { node.swap_read_write_datasets! }.to raise_error(NotImplementedError)
    end
  end

  describe '#import' do
    it 'raises an error' do
      expect { node.import }.to raise_error(NotImplementedError)
    end
  end

  describe '#drop_dataset!' do
    it 'raises an error' do
      expect { node.drop_dataset! }.to raise_error(NotImplementedError)
    end
  end

  let(:node) {
    params = make_data_node_params('external_node')
    Dataflow::Nodes::ReadOnlyDataNode.create(params)
  }
end
