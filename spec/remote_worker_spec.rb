require 'spec_helper'

class DummyCompute < Dataflow::Nodes::ComputeNode

  field :compute_impl_count, default: 0
  field :compute_batch_count, default: 0

  field :raise_on_compute, default: false

  def compute_impl
    raise 'some error' if raise_on_compute

    self.compute_impl_count += 1
    save
  end

  def compute_batch(records:)
    self.compute_batch_count += 1
    save
    []
  end
end


describe Dataflow::RemoteWorker do
  let(:compute_node) {
    DummyCompute.create(
      name: 'remote_worker_test',
      dependency_ids: [deps_node.id],
      data_node_id: data_node.id
    )
  }
  let(:data_node) {
    make_data_node('remote_worker_dataset')
  }
  let(:deps_node) {
    make_data_node('remote_worker_dataset')
  }

  let(:compute_message) {
    {
      'msg_id' => 0,
      'is_batch' => false,
      'node_id' => compute_node._id.to_s,
      'execution_uuid' => compute_node.execution_uuid.to_s,
    }
  }

  let(:batch_compute_message) {
    {
      'msg_id' => 1,
      'is_batch' => true,
      'params' => compute_node.make_batch_params[1],
      'node_id' => compute_node._id.to_s,
      'execution_uuid' => compute_node.execution_uuid.to_s,
    }
  }


  before do
    deps_node.add(records: [{ id: 1 }, { id: 2 }])
    compute_node.send(:acquire_computing_lock!)
  end

  context 'with a valid execution uuid' do
    it 'processes a message (non-batch)' do
      response = Dataflow::RemoteWorker.process(compute_message)

      compute_node.reload
      expect(compute_node.compute_impl_count).to eq(1)
      expect(response).to eq({msg_id: 0})
    end

    it 'processes a message (batch)' do
      response = Dataflow::RemoteWorker.process(batch_compute_message)

      compute_node.reload
      expect(compute_node.compute_batch_count).to eq(1)
      expect(response).to eq({msg_id: 1})
    end

    it 'returns an error that may happen during computation' do
      compute_node.raise_on_compute = true
      compute_node.save

      response = Dataflow::RemoteWorker.process(compute_message)

      compute_node.reload
      expect(compute_node.compute_impl_count).to eq(0)
      expect(response[:error][:message].present?).to eq true
      expect(response[:error][:backtrace].present?).to eq true
    end

    it 'returns an error when the node is not found' do
      compute_message['node_id'] = 'invalid'
      response = Dataflow::RemoteWorker.process(compute_message)

      expect(response[:error][:message].present?).to eq true
      expect(response[:error][:backtrace].present?).to eq true
    end
  end

  context 'without a valid execution uuid' do
    it 'skips processing the message' do
      compute_message['execution_uuid'] = 'invalid'
      response = Dataflow::RemoteWorker.process(compute_message)

      compute_node.reload
      expect(compute_node.compute_impl_count).to eq(0)
      expect(response).to eq(nil)
    end
  end

end
