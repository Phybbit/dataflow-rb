# frozen_string_literal: true

Time.class_eval do
  def to_msgpack(out = '')
    iso8601.to_msgpack(out)
  end
end

DateTime.class_eval do
  def to_msgpack(out = '')
    iso8601.to_msgpack(out)
  end
end

Date.class_eval do
  def to_msgpack(out = '')
    iso8601.to_msgpack(out)
  end
end
