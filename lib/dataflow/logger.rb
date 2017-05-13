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
      return if ENV['DATAFLOW_SKIP_LOGGING']
      now = DateTime.now.strftime('%y-%m-%d %H:%M:%S')
      message = "[#{now}][#{trace_id}] #{prefix} | #{str}"
      logger_impl = @@impl
      logger_impl.log(message)
    end

    def error(error:, custom_message: '')
      first_line = "[ERROR => #{error.class}: '#{error.message}']"
      first_line += " #{custom_message}" if custom_message.present?
      first_line += ' Backtrace: '
      log(first_line)
      log('--')
      (error.backtrace || []).each_with_index { |line, idx| log("#{idx}: #{line}") }
    end

    def trace_id
      (Process.pid + Thread.current.object_id).to_s(16)[-8..-1]
    end

    class LoggerImpl
      def log(message)
        puts message
      end
    end
  end
end
