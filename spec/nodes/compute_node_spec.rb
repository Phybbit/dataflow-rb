# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::ComputeNode, type: :model do
  # dummy computed node implementation class
  class NOOPComputeNode < Dataflow::Nodes::ComputeNode
    def compute_batch(records:)
      records
    end
  end

  describe '#recompute' do
    it 'recomputes dependencies that need it' do
      # make sure the older dependency needs to be updated
      allow(older_dependency).to receive(:updated?).and_return(false)
      allow(compute_node).to receive(:dependencies)
        .and_return([newer_dependency, older_dependency, raw_data_dependency])

      compute_node.recompute

      # and that it has indeed been updated
      older_dependency.reload
      expect(older_dependency.updated_at).to be > older_date
    end

    it 'updates the current node' do
      previous_updated_at = compute_node.updated_at
      compute_node.recompute

      compute_node.reload
      expect(compute_node.updated_at).to be > previous_updated_at
    end
  end

  describe '#is_updated?' do
    it 'returns false if any dependency was updated after our last update' do
      expect(compute_node.updated?).to eq false
    end

    it 'returns false if there is no updated_at' do
      params = { name: 'new_node' }
      new_node = Dataflow::Nodes::ComputeNode.create(params)
      expect(new_node.updated?).to eq false
    end

    it 'returns false if a dependency is not updated itself' do
      params = { name: 'new_node', last_compute_starting_time: middle_date }
      new_node = Dataflow::Nodes::ComputeNode.create(params)
      allow(new_node).to receive(:dependencies).and_return([older_dependency])
      allow(older_dependency).to receive(:updated?).and_return(false)
      expect(new_node.updated?).to eq false
    end

    it 'returns true after recomputing' do
      compute_node.recompute

      compute_node.reload
      expect(compute_node.updated?).to eq true
    end
  end

  describe '#validate!' do
    class ExactDepsNode < Dataflow::Nodes::ComputeNode
      ensure_dependencies exactly: 1
    end

    class AtLeastDepsNode < Dataflow::Nodes::ComputeNode
      ensure_dependencies min: 1
    end

    class AtMostDepsNode < Dataflow::Nodes::ComputeNode
      ensure_dependencies max: 1
    end

    class EnsureDataNodeExists < Dataflow::Nodes::ComputeNode
      ensure_data_node_exists
    end

    let (:no_deps_node)       { ExactDepsNode.create!(name: 'no_deps') }
    let (:valid_deps_node)    { ExactDepsNode.create!(name: 'with_deps', dependency_ids: [compute_node]) }
    let (:at_least_deps_node) { AtLeastDepsNode.create!(name: 'at_least_deps') }
    let (:valid_at_least)     { AtLeastDepsNode.create!(name: 'at_least_deps', dependency_ids: [compute_node]) }
    let (:at_most_deps_node)  { AtMostDepsNode.create!(name: 'at_most_deps', dependency_ids: [compute_node, compute_node]) }
    let (:valid_at_most_deps) { AtMostDepsNode.create!(name: 'at_most_deps', dependency_ids: [compute_node]) }
    let (:cyclic_dep) { cyclic_dep = Dataflow::Nodes::ComputeNode.create!(name: 'dep_lv1', dependency_ids: [compute_node]) }
    let (:no_data_node_presence) { EnsureDataNodeExists.create!(name: 'no_data_node') }
    let (:valid_data_node_presence) { EnsureDataNodeExists.create!(name: 'no_data_node', data_node_id: make_data_node('data')) }

    it 'checks for cyclic dependencies' do
      compute_node.dependency_ids = [cyclic_dep._id]
      compute_node.save!

      expect { compute_node.validate! }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'checks for exact dependencies' do
      expect { no_deps_node.validate! }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
      expect { valid_deps_node.validate! }.not_to raise_error
    end

    it 'checks for at least dependencies' do
      expect { at_least_deps_node.validate! }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
      expect { valid_at_least.validate! }.not_to raise_error
    end

    it 'checks for at most dependencies' do
      expect { at_most_deps_node.validate! }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
      expect { valid_at_most_deps.validate! }.not_to raise_error
    end

    it 'validates before computing' do
      expect { no_deps_node.compute }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
    end

    it 'checks for data node presence' do
      expect { no_data_node_presence.validate! }.to raise_error(Dataflow::Errors::InvalidConfigurationError)
      expect { valid_data_node_presence.validate! }.not_to raise_error
    end
  end

  describe 'needs_automatic_recomputing?' do
    it 'returns false if the recompute_interval is 0' do
      compute_node.recompute_interval = 0
      expect(compute_node.needs_automatic_recomputing?).to be false
    end

    it 'returns false if the updated_at still within the interval' do
      compute_node.recompute_interval = 1
      Timecop.freeze(compute_node.updated_at) do
        expect(compute_node.needs_automatic_recomputing?).to be false
      end
    end

    it 'returns true if the node has never been computed' do
      compute_node.recompute_interval = 1
      Timecop.freeze(compute_node.last_compute_starting_time) do
        compute_node.last_compute_starting_time = nil
        expect(compute_node.needs_automatic_recomputing?).to be true
      end
    end

    it 'returns true if the updated_at is more than the inveral' do
      compute_node.recompute_interval = 1
      Timecop.freeze(compute_node.updated_at + 2.seconds) do
        expect(compute_node.needs_automatic_recomputing?).to be true
      end
    end

    it 'returns false if the node is currently computing' do
      compute_node.send(:acquire_computing_lock!)
      expect(compute_node.needs_automatic_recomputing?).to be false
    end
  end

  describe '#compute' do
    it 'acquires a computing lock by setting the compute state to computing' do
      allow(compute_node).to receive(:compute_impl) do
        compute_node.reload
        expect(compute_node.computing_state).to eq 'computing'
      end

      compute_node.compute
      expect(compute_node).to have_received(:compute_impl)
    end

    it 'releases the a computing lock by clearing the compute state' do
      compute_node.compute
      compute_node.reload
      expect(compute_node.computing_state).to eq nil
    end

    it 'awaits another process to finish if the node is currently computing' do
      compute_node.computing_state = 'computing'
      compute_node.save!

      allow(compute_node).to receive(:await_computing!)
      compute_node.compute
      expect(compute_node).to have_received(:await_computing!)
    end

    it 'sets the computing started time' do
      Timecop.freeze('2016-01-01T15:00:00Z') do
        allow(compute_node).to receive(:compute_impl) do
          compute_node.reload
          expect(compute_node.computing_started_at).to eq '2016-01-01T15:00:00Z'.to_time
        end

        compute_node.compute
        expect(compute_node).to have_received(:compute_impl)
      end
    end

    it 'sets the computing_started_at to nil after completion' do
      compute_node.compute
      compute_node.reload
      expect(compute_node.computing_started_at).to eq nil
    end

    it 'sets an heartbeat upon starting computing' do
      Timecop.freeze('2016-01-01T15:00:00Z') do
        allow(compute_node).to receive(:compute_impl) do
          compute_node.reload
          expect(compute_node.last_heartbeat_time).to eq '2016-01-01T15:00:00Z'.to_time
        end

        compute_node.compute
        expect(compute_node).to have_received(:compute_impl)
      end
    end

    context 'index creation' do
      let(:indexes) { [{ 'key' => ['id'], 'unique' => true }, { 'key' => ['other'] }] }

      before do
        compute_node.data_node.indexes = indexes
        compute_node.save
      end

      it 'only creates unique indexes before computing' do
        allow(compute_node).to receive(:compute_impl) do
          expect(compute_node.data_node.info(write_dataset: true)[:effective_indexes]).to eq([{ 'key' => ['id'], 'unique' => true }])
        end

        compute_node.compute
        expect(compute_node).to have_received(:compute_impl)
      end

      it 'after computing it creates the other indexes' do
        compute_node.compute
        expect(compute_node.data_node.info[:effective_indexes]).to eq(indexes)
      end
    end

    context 'do not clear on recompute' do
      class AddOneRecord < Dataflow::Nodes::ComputeNode
        ensure_dependencies min: 0
        ensure_data_node_exists

        def compute_impl
          data_node.add(records: [{ a: 1 }])
        end
      end

      it 'does not reset the state between recomputes' do
        node = AddOneRecord.create(
          name: 'clear_on_compute_false',
          clear_data_on_compute: false,
          data_node_id: make_data_node('data')
        )

        node.compute(force_compute: true)
        node.compute(force_compute: true)

        expect(node.count).to eq 2
      end
    end
  end

  describe '#computing_started' do
    it 'supports registering on a global-level' do
      called = 0
      Dataflow::Nodes::ComputeNode.computing_started { called += 1 }
      compute_node.compute
      expect(called).to eq 1
    end

    it 'supports registering on a node-level' do
      called = 0
      compute_node.computing_started { called += 1 }
      compute_node.compute
      expect(called).to eq 1
    end

    it 'is independent of the class used on a global-level' do
      called = 0
      Dataflow::Nodes::ComputeNode.computing_started { called += 1 }
      node_params = {
        name: 'a_descendant_node',
        dependency_ids: [newer_dependency, older_dependency]
      }
      node = NOOPComputeNode.create(node_params)
      node.compute
      expect(called).to eq 1
    end
  end

  describe '#computing_finished' do
    it 'is called' do
      called = 0
      Dataflow::Nodes::ComputeNode.computing_finished { called += 1 }
      compute_node.compute
      expect(called).to eq 1
    end
  end

  describe '#required_by' do
    before do
      # make sure these nodes are created
      compute_node
      newer_dependency
    end

    it 'finds which nodes are currently using the given node as a dependency' do
      expect(newer_dependency.required_by).to eq([node: compute_node, type: 'dependency'])
    end

    it 'finds which nodes are currently using the given node as a dataset' do
      expect(newer_dependency.data_node.required_by).to eq([node: newer_dependency, type: 'dataset'])
    end
  end

  describe '.properties' do
    it 'includes parent properties' do
      expect(NOOPComputeNode.properties[:name]).not_to be nil
    end
  end

  let (:older_date)   { Time.parse('2014-01-01') }
  let (:middle_date)  { Time.parse('2015-01-01') }
  let (:newer_date)   { Time.parse('2016-01-01') }

  let(:newer_dependency) do
    params = {
      name: 'newer_data',
      last_compute_starting_time: newer_date,
      data_node_id: make_data_node('newer_data_node'),
      dependency_ids: [raw_data_dependency]
    }
    NOOPComputeNode.create(params)
  end
  let(:older_dependency) do
    params = {
      name: 'older_data',
      last_compute_starting_time: older_date,
      data_node_id: make_data_node('older_data_node'),
      dependency_ids: [raw_data_dependency]
    }
    NOOPComputeNode.create(params)
  end
  let(:raw_data_dependency) do
    node = make_data_node('raw_data', updated_at: newer_date)
    node.add(records: [{ 'id' => '1' }])
    node
  end
  let(:compute_node) do
    params = {
      name: 'compute_node',
      last_compute_starting_time: middle_date,
      data_node_id: make_data_node('output_node'),
      dependency_ids: [newer_dependency, older_dependency, raw_data_dependency]
    }
    NOOPComputeNode.create(params)
  end
end
