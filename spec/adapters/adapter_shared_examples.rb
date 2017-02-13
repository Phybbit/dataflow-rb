require 'spec_helper'

def define_hash_key_method(use_sym)
  # use define_method instead of def to use the 'use_sym'
  # without passing it explicitily
  define_method :hash_key do |key|
    use_sym ? key.to_sym : key
  end
end

RSpec.shared_examples 'adapter #find' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#find' do
    it 'retrieves one record' do
      record = adapter.find
      expect(record.is_a?(Hash)).to eq true
    end

    it 'retrieves the correct document' do
      record = adapter.find(where: {'id' => 2})
      expect(record[hash_key('value')]).to eq 2
    end

    it 'support sorting' do
      record = adapter.find(where: {'id' => 1}, sort: {value: -1})
      expect(record[hash_key('value')]).to eq 3
    end

    it 'supports offset' do
      record = adapter.find(where: {'id' => 1}, offset: 1)
      expect(record[hash_key('value')]).to eq 2
    end

    it 'uses a default order when using offset without sort' do
      record = adapter.find(offset: 2)
      expect(record[hash_key('value')]).to eq 3
    end

    it 'supports all params at once' do
      record = adapter.find(where: {'id' => 1}, offset: 1, sort: {value: -1}, fields: ['value'])
      expect(record).to eq({hash_key('value') => 2})
    end
  end
end


RSpec.shared_examples 'adapter #all' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#all' do
    it 'retrieves all document' do
      records = adapter.all
      expect(records.count).to eq dummy_data.count
    end

    it 'returns an array' do
      records = adapter.all
      expect(records.class).to eq Array
    end

    it 'retrieves documents with matching data' do
      records = adapter.all
      records.each do |record|
        expect(dummy_data.include?(record)).to eq true
      end
    end

    context 'queries' do
      it 'supports =' do
        records = adapter.all(where: {'id' => 1})
        expect(records.count).to eq 3
        records.each do |record|
          expect(record[hash_key('id')]).to eq 1
        end
      end

      it 'supports IN' do
        records = adapter.all(where: {'id' => [2, 3]})
        expect(records.count).to eq 2
        expect(records[0][hash_key('id')]).to eq 2
        expect(records[1][hash_key('id')]).to eq 3
      end

      it 'supports !=' do
        records = adapter.all(where: {'id' => { '!=' => 1 }})
        expect(records.count).to eq 2
        expect(records[0][hash_key('id')]).to eq 2
        expect(records[1][hash_key('id')]).to eq 3
      end

      it 'supports NOT IN' do
        records = adapter.all(where: {'id' => { '!=' => [1, 2] }})
        expect(records.count).to eq 1
        expect(records[0][hash_key('id')]).to eq 3
      end

      it 'supports >=' do
        records = adapter.all(where: {'id' => { '>=' => 2 }})
        expect(records.count).to eq 2
        expect(records[0][hash_key('id')]).to eq 2
        expect(records[1][hash_key('id')]).to eq 3
      end

      it 'supports >' do
        records = adapter.all(where: {'id' => { '>' => 1 }})
        expect(records.count).to eq 2
        expect(records[0][hash_key('id')]).to eq 2
        expect(records[1][hash_key('id')]).to eq 3
      end

      it 'supports <=' do
        records = adapter.all(where: {'id' => { '<=' => 1 }})
        expect(records.count).to eq 3
        records.each do |record|
          expect(record[hash_key('id')]).to eq 1
        end
      end

      it 'supports <' do
        records = adapter.all(where: {'id' => { '<' => 2 }})
        expect(records.count).to eq 3
        records.each do |record|
          expect(record[hash_key('id')]).to eq 1
        end
      end

      it 'supports dates' do
        records = adapter.all(where: {'updated_at' => { '<=' => '2016-01-16'.to_datetime, '>=' => '2016-01-14'.to_datetime }})
        expect(records.count).to eq 1
        expect(records[0][hash_key('id')]).to eq 1
        expect(records[0][hash_key('value')]).to eq 2
      end

      it 'supports multiple operators' do
        records = adapter.all(where: {'id' => { '>' => 1, '<' => 3 }})
        expect(records.count).to eq 1
        expect(records[0][hash_key('id')]).to eq 2
      end
    end

    it 'support sorting ASC' do
      records = adapter.all(where: {'id' => 1}, sort: {'value' => 1})
      expect(records[0][hash_key('value')]).to eq 1
    end

    it 'support sorting DESC' do
      records = adapter.all(where: {'id' => 1}, sort: {'value' => -1})
      expect(records[0][hash_key('value')]).to eq 3
    end

    it 'uses a default order when using offset without sort' do
      records = adapter.all(offset: 2)
      expect(records.count).to eq 3
      expect(records[0][hash_key('value')]).to eq 3
      expect(records[1][hash_key('value')]).to eq 2
      expect(records[2][hash_key('value')]).to eq 3
    end

    it 'supports offset' do
      records = adapter.all(where: {'id' => 1}, offset: 1)
      expect(records.count).to eq 2
    end

    it 'supports limit' do
      records = adapter.all(where: {'id' => 1}, limit: 1)
      expect(records.count).to eq 1
    end

    it 'keeps the _id if it was supplied in the fields' do
      records = adapter.all(fields: ['_id'])
      records.each do |record|
        expect(record[hash_key('_id')].present?).to eq true
      end
    end

    it 'supports all params at once' do
      records = adapter.all(where: {'id' => 1}, offset: 1, limit: 2, sort: {value: -1}, fields: ['value'])
      expect(records).to eq([{hash_key('value') => 2}, {hash_key('value') => 1}])
    end
  end
end

RSpec.shared_examples 'adapter #all_paginated' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#all_paginated' do
    it 'retrieves all document' do
      res = adapter.all_paginated
      expect(res['data'].count).to eq dummy_data.count
    end

    it 'also works with an empty cursor' do
      res = adapter.all_paginated(cursor: '')
      expect(res['data'].count).to eq dummy_data.count
    end

    it 'returns a cursor' do
      res = adapter.all_paginated
      expect(res.key?('next_cursor')).to eq true
    end

    it 'returns 0 as a cursor if there is less records than requested' do
      res = adapter.all_paginated
      expect(res['next_cursor']).to eq ''
    end

    it 'returns an hash' do
      res = adapter.all_paginated
      expect(res.class).to eq Hash
    end

    it 'retrieves documents with matching data' do
      res = adapter.all_paginated
      res['data'].each do |record|
        expect(dummy_data.include?(record)).to eq true
      end
    end

    it 'supports paging through the dataset' do
      # add enough data so that the first fetch does not consume everything
      p 'adding data to the DB... (it may take a while)'
      adapter.save(records: dummy_data * 20000)

      p 'paginating through the data...'
      res = adapter.all_paginated(where: {'id' => { "<=" => 2}})
      expect(res['data'].all? { |x| x['id'] <= 2 }).to eq true
      expect(res['next_cursor'].present?).to eq true
      expect(res['next_cursor'].class).to eq String

      cursor = res['next_cursor']

      loop do
        res = adapter.all_paginated(cursor: cursor)
        expect(res['data'].all? { |x| x['id'] <= 2 }).to eq true
        break if res['next_cursor'].blank?
      end

      # if we keep calling on a terminated cursor, it returns empty data/cursor
      empty_response = {'data' => [], 'next_cursor' => ''}
      expect(adapter.all_paginated(cursor: cursor)).to eq(empty_response)
    end

  end
end


RSpec.shared_examples 'adapter #count' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#count' do
    it 'returns the total amount' do
      expect(adapter.count).to eq dummy_data.count
    end

    it 'returns the total count with args' do
      expect(adapter.count(where: {hash_key('id') => 2})).to eq 1
    end
  end
end


RSpec.shared_examples 'adapter #save' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#save' do
    it 'adds batch data to the db' do
      adapter.save(records: dummy_data)
      expect(adapter.count).to eq dummy_data.count
    end

    it 'keeps adding even if one entry fails due to duplicates' do
      adapter.create_indexes
      adapter.save(records: dummy_data)

      new_data = dummy_data.deep_dup
      new_data[-1][hash_key('updated_at')] = '2020-01-01'.to_time # only the last data changed
      adapter.save(records: new_data)

      expect(adapter.count).to eq(dummy_data.count + 1)
    end
  end
end


RSpec.shared_examples 'adapter #delete' do |use_sym: false|
  define_hash_key_method(use_sym)

  describe '#remove' do
    it 'removes data from the db' do
      adapter.save(records: dummy_data)
      adapter.delete(where: {'id' => 1})
      expect(adapter.count).to eq 2
    end
  end
end
