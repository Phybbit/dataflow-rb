require 'spec_helper'

RSpec.describe Dataflow::Nodes::Mixin::AddInternalTimestamp, type: :mixin do
  describe '#add_internal_timestamp' do
    let (:time) { '2016-01-01T00:00:00Z' }

    it 'adds an internal updated_at field' do
      Timecop.freeze(time) do
        node.add_internal_timestamp(records: records)
        expect(records).to eq(expected_records)
      end
    end

    let (:node) {
      params = make_data_node_params('raw_data_unique',
        index_key: 'unique_id', rename_dotted_fields_in: ['conversion_specs']
      )
      Dataflow::Nodes::UpsertNode.create(params)
    }

    let (:records) {
      records = [{
        'id' => '1',
      }]
    }
    let(:expected_records) {
      [{
        'id' => '1',
        '_mojaco_updated_at' => time.to_time
      }]
    }
  end
end
