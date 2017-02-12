# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Dataflow::Nodes::Filter::DropWhileNode, type: :model do
  describe '#compute' do
    before do
      base_node.add(records: records)
    end

    it 'drops on the left' do
      node.drop_mode = 'left'
      node.compute
      expect_dataset_are_equal(node.all, expected_dataset_left_mode, key: %w(id start_time))
    end

    it 'drops on the right' do
      node.drop_mode = 'right'
      node.compute
      expect_dataset_are_equal(node.all, expected_dataset_right_mode, key: %w(id start_time))
    end

    it 'drops on the left and on the right' do
      node.drop_mode = 'both'
      node.compute
      expect_dataset_are_equal(node.all, expected_dataset_both_mode, key: %w(id start_time))
    end

    it 'supports sort desc' do
      node.drop_mode = 'left'
      node.sort_asc = false
      node.compute
      # should equal the right mode
      expect_dataset_are_equal(node.all, expected_dataset_right_mode, key: %w(id start_time))
    end
  end

  let(:records) do
    [
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] }, # will be dropped on left/both
        'start_time' => '2016-01-03T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [1] }, # kept
        'start_time' => '2016-01-05T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [2] }, # kept
        'start_time' => '2016-01-04T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] }, # dropped on right/both
        'start_time' => '2016-01-06T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => nil }, # dropped on left/both
        'start_time' => '2016-01-02T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] }, # dropped on left/both
        'start_time' => '2016-01-01T00:00:00Z'
      },
      {
        'id' => 'different_id',
        'hash' => { 'field' => [1] }, # kept (it is a different id)
        'start_time' => '2016-01-01T00:00:00Z'
      },
      {
        'id' => 'different_id_2',
        'hash' => { 'field' => [0] }, # will always be dropped completely
        'start_time' => '2016-01-01T00:00:00Z'
      }
    ]
  end

  let(:expected_dataset_both_mode) do
    [
      {
        'id' => 'abc',
        'hash' => { 'field' => [2] },
        'start_time' => '2016-01-04T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [1] },
        'start_time' => '2016-01-05T00:00:00Z'
      },
      {
        'id' => 'different_id',
        'hash' => { 'field' => [1] }, # kept (it is a different id)
        'start_time' => '2016-01-01T00:00:00Z'
      }
    ]
  end

  let(:expected_dataset_left_mode) do
    [
      {
        'id' => 'abc',
        'hash' => { 'field' => [2] }, # kept
        'start_time' => '2016-01-04T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [1] }, # kept
        'start_time' => '2016-01-05T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] }, # kept
        'start_time' => '2016-01-06T00:00:00Z'
      },
      {
        'id' => 'different_id',
        'hash' => { 'field' => [1] }, # kept (it is a different id)
        'start_time' => '2016-01-01T00:00:00Z'
      }
    ]
  end

  let(:expected_dataset_right_mode) do
    [
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] },
        'start_time' => '2016-01-01T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => nil },
        'start_time' => '2016-01-02T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [0] },
        'start_time' => '2016-01-03T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [2] }, # kept
        'start_time' => '2016-01-04T00:00:00Z'
      },
      {
        'id' => 'abc',
        'hash' => { 'field' => [1] }, # kept
        'start_time' => '2016-01-05T00:00:00Z'
      },
      {
        'id' => 'different_id',
        'hash' => { 'field' => [1] }, # kept (it is a different id)
        'start_time' => '2016-01-01T00:00:00Z'
      }
    ]
  end

  let(:base_node) { make_data_node('data') }

  let(:node) do
    Dataflow::Nodes::Filter::DropWhileNode.create(
      name: 'drop_while_node',
      dependency_ids: [base_node.id],
      id_key: 'id',
      sort_by: 'start_time',
      field: 'hash|field|0',
      op: 'le',
      value: 0,
      data_node_id: make_data_node('data')
    )
  end
end
