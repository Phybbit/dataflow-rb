require 'spec_helper'

RSpec.describe Dataflow::Nodes::SnapshotNode, type: :model do

  describe '#indexes' do
    it 'creates indexes automatically: id' do
      expect(node.indexes.include?({'key' => 'id'})).to eq true
    end

    it 'creates indexes automatically: updated_at' do
      expect(node.indexes.include?({'key' => 'updated_at'})).to eq true
    end

    it 'creates indexes automatically: unique (id, updated_at)' do
      unique_key = { 'key' => ['id', 'updated_at'], 'unique' => true}
      expect(node.indexes.include?(unique_key)).to eq true
    end
  end

  describe '#add' do
    it 'transforms the updated_at to a time key' do
      node.add(records: base_data)
      expect(node.all[0]['updated_at'].class).to be Time
    end

    it 'supports timestamps when transforming updated_at to a time key' do
      node.add(records: base_data_with_timestamp)
      expect(node.all[0]['updated_at'].class).to be Time
    end

    it 'does not add records that did not change' do
      node.add(records: base_data.deep_dup)
      node.add(records: base_data.deep_dup)
      # only adds once
      expect_snapshot_content_equal_to(base_data)
    end

    def expect_snapshot_content_equal_to(expected_data)
      expected_data.each { |x| x['updated_at'] = Time.parse(x['updated_at']).utc }
      expect(node.count).to eq(expected_data.count)
      node.all.each { |x|
        expect(expected_data.include?(x)).to eq true
      }
    end

    it 'adds temporal snapshots' do
      node.add(records: base_data.deep_dup)
      node.add(records: values_changed_data.deep_dup)

      expected_data = base_data + values_changed_data
      expect_snapshot_content_equal_to(expected_data)
    end

    it 'does not add the current record if only the update key changed' do
      node.add(records: base_data.deep_dup)
      node.add(records: timestamp_changed_data.deep_dup)

      expect_snapshot_content_equal_to(base_data)
    end

    it 'compares with the record that has the last updated_at' do
      node.add(records: base_data.deep_dup)
      node.add(records: values_changed_data.deep_dup)
      node.add(records: another_timestamp_changed_data.deep_dup)

      expected_data = base_data + values_changed_data
      expect_snapshot_content_equal_to(expected_data)
    end


    it 'supports _mojaco_updated_at as a updated_at time key' do
      Timecop.freeze('2016-01-01T00:00:00Z') do
        node_mojaco_timestamp.add(records: base_data_without_timestamp)
      end

      Timecop.freeze('2016-01-01T00:00:11Z') do
        node_mojaco_timestamp.add(records: values_changed_without_timestamp)
      end

      Timecop.freeze('2016-01-01T00:00:15Z') do
        # this should be ignored as the data has not changed
        node_mojaco_timestamp.add(records: values_changed_without_timestamp)
      end

      data = node_mojaco_timestamp.all
      expect(data.count).to eq 2
      expect(data[0]).to eq({'id' => 2, 'some_complex_value' => {'s' => 2}, '_mojaco_updated_at' => '2016-01-01T00:00:00Z'.to_time})
      expect(data[1]).to eq({'id' => 2, 'some_complex_value' => {'s' => 3}, '_mojaco_updated_at' => '2016-01-01T00:00:11Z'.to_time})
    end
  end

  context 'use mojaco timestamp' do
    it 'adds internal update at timestamps' do
      time = '2016-01-01T00:00:00Z'
      Timecop.freeze(time) do
        node.use_internal_timestamp = true
        node.add(records: [{'id' => 1, 'updated_at' => '2016-01-01'}])
        expect(node.all).to eq([{'id' => 1, 'updated_at' => '2016-01-01'.to_time, '_mojaco_updated_at' => time.to_time}])
      end
    end
  end


  let (:base_data) {
    [{
      'id' => 2,
      'updated_at' => '2015-01-01',
      'some_complex_value' => {'s' => 2},
    }]
  }

  let (:base_data_with_timestamp) {
    [{
      'id' => 2,
      'updated_at' => 1461826750,
      'some_complex_value' => {'s' => 2},
    }]
  }

  let (:timestamp_changed_data) {
    [{
      'id' => 2,
      'updated_at' => '2016-01-01',       # changed! => ignored
      'some_complex_value' => {'s' => 2}, # not changed!
    }]
  }

  let (:values_changed_data) {
    [{
      'id' => 2,
      'updated_at' => '2017-01-01',       # changed!
      'some_complex_value' => {'s' => 3}, # changed! => kept!
    }]
  }

  let (:another_timestamp_changed_data) {
    [{
      'id' => 2,
      'updated_at' => '2018-01-01',       # changed! => ignored!
      'some_complex_value' => {'s' => 3}, # not changed!
    }]
  }

  let (:base_data_without_timestamp) {
    [{
      'id' => 2,
      'some_complex_value' => {'s' => 2},
    }]
  }

  let (:values_changed_without_timestamp) {
    [{
      'id' => 2,
      'some_complex_value' => {'s' => 3},
    }]
  }

  let (:node) {
    params = make_data_node_params('snapshot',
      index_key: 'id',
      updated_at_key: 'updated_at',
      use_internal_timestamp: false,
    )
    Dataflow::Nodes::SnapshotNode.create(params)
  }

  let (:node_mojaco_timestamp) {
    params = make_data_node_params('snapshot',
      index_key: 'id',
      updated_at_key: '_mojaco_updated_at',
      use_internal_timestamp: true,
    )
    Dataflow::Nodes::SnapshotNode.create(params)
  }
end
