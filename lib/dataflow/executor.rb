# frozen_string_literal: true
require 'bunny'
require 'json'
require 'thread'

module Dataflow
  class Executor
    class << self
      def execute(node)
        case node.execution_model
        when :remote
          execute_remote_computation(node: node, is_batch_execution: false)
        when :remote_batch
          execute_remote_computation(node: node, is_batch_execution: true)
        when :local
          node.execute_local_computation
        else
          raise ArgumentError, "Unknown execution model #{execution_model}"
        end
      end

      def execute_remote_computation(node:, is_batch_execution:)
        execution_uuid = node.execution_uuid
        raise ArgumentError, "Expected execution uuid to be set on '#{node.name}' (##{node._id})" unless execution_uuid.present?

        logger.log("Started processing '#{node.name}'")
        conn, channel, completion_queue = open_communication_channel
        logger.log("Opened a completion queue for '#{node.name}': #{completion_queue.name}")

        messages = send_execution_messages(channel, node, is_batch_execution, completion_queue.name)
        error_data = await_execution_completion(node, completion_queue, messages.count)
        logger.log("Finished processing '#{node.name}'")

        raise Errors::RemoteExecutionError.new(error_data['message'], error_data['backtrace']) if error_data
      ensure
        conn&.close
      end

      def open_communication_channel
        conn = Bunny.new(ENV['MOJACO_RABBITMQ_URI'])
        conn.start

        ch = conn.create_channel
        completion_queue = ch.queue('', exclusive: true)

        [conn, ch, completion_queue]
      end

      def send_execution_messages(channel, node, is_batch_execution, completion_queue_name)
        execution_params = make_execution_params(node, is_batch_execution, completion_queue_name)

        execution_queue = channel.queue(node.execution_queue)
        execution_params.each do |exec_params|
          execution_queue.publish(exec_params.to_json)
        end

        execution_params
      end

      def make_execution_params(node, is_batch_execution, completion_queue_name)
        execution_params = if is_batch_execution
                             node.make_batch_params
                           else
                             [{}]
                           end

        execution_params.each_with_index.map do |params, idx|
          {
            msg_id: idx,
            node_id: node._id.to_s,
            is_batch: is_batch_execution,
            params: params,
            execution_uuid: node.execution_uuid.to_s,
            completion_queue_name: completion_queue_name
          }
        end
      end

      def await_execution_completion(node, completion_queue, expected_completion_count)
        completed_message_indexes = []
        unblock = Queue.new

        consumer = completion_queue.subscribe do |_delivery_info, _properties, payload|
          data = JSON.parse(payload)
          unblock.enq(data['error']) if data['error'].present?

          # Support adding the data to the compute's data_node is the
          # remote process returns anything.
          node.data_node&.add(records: data['data']) if data['data'].present?

          completed_message_indexes << data['msg_id']
          if completed_message_indexes.count == expected_completion_count
            unblock.enq(false)
          end
        end

        error_data = unblock.deq
        consumer.cancel

        error_data
      end

      def logger
        @logger ||= Dataflow::Logger.new(prefix: 'Executor')
      end
    end
  end
end
