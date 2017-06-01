# frozen_string_literal: true
module Dataflow
  module Errors
    class RemoteExecutionError < StandardError

      def initialize(msg, backtrace)
        super(msg)
        set_backtrace(backtrace)
      end

    end
  end
end
