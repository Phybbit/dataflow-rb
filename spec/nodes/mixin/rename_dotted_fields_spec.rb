require 'spec_helper'

RSpec.describe Dataflow::Nodes::Mixin::RenameDottedFields, type: :mixin do
  describe '#rename_dotted_fields' do
    it 'converts dotted fields' do
      node.rename_dotted_fields(records: records)
      expect(records).to eq(expected_records)
    end

    it 'converts dotted fields automatically during the add operation' do
      node.add(records: records)
      expect(node.all).to eq(expected_records)
    end

    it 'supports traversing the whole document' do
      node.rename_dotted_fields_in = ['.']
      node.add(records: records)
      expect(node.all).to eq(expected_records)
    end

    let (:node) {
      params = make_data_node_params('raw_data_unique',
        index_key: 'unique_id', rename_dotted_fields_in: ['conversion_specs'], use_internal_timestamp: false
      )
      Dataflow::Nodes::UpsertNode.create(params)
    }

    let (:records) {
      records = [{
        'conversion_specs' => [{'action.type' => 'converted', 'wall.post' => 'converted'}],
        'untouched' => 'value'
      }]
    }
    let(:expected_records) {
      [{
        'conversion_specs' => [{'action_type' => 'converted', 'wall_post' => 'converted'}],
        'untouched' => 'value'
      }]
    }
  end
end
