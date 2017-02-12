# frozen_string_literal: true
module Dataflow
  class Logger
    attr_accessor :prefix
    attr_accessor :use_notifications

    def initialize(prefix:, use_notifications: false)
      @prefix = prefix
      @use_notifications = use_notifications
      @@impl = LoggerImpl.new
    end

    def log(str)
      return if ENV['RACK_ENV'] == 'test'
      now = DateTime.now.strftime('%y-%m-%d %H:%M:%S')
      message = "[#{now}] #{prefix} :: #{str}"
      logger_impl = @@impl
      logger_impl.log(message)
    end

    class LoggerImpl
      def log(message)
        puts message
      end
    end
  end
end
