# frozen_string_literal: true
require 'bunny'
require 'json'

module Dataflow
  class RemoteWorker
    class << self
      def work(work_queue_name = 'dataflow.ruby')
        conn = Bunny.new(ENV['MOJACO_RABBITMQ_URI'])
        conn.start

        ch = conn.create_channel
        queue = ch.queue(work_queue_name)
        ch.prefetch(1)

        logger.log("Accepting work on #{work_queue_name}...")

        queue.subscribe(block: true, manual_ack: true) do |delivery_info, _properties, payload|
          data = JSON.parse(payload)
          response = process(data)
          if response.present?
            ch.default_exchange.publish(response.to_json, routing_key: data['completion_queue_name'])
          end
          ch.ack(delivery_info.delivery_tag)
        end
      ensure
        conn.close
        logger.log('Connection closed, stopped accepting work.')
      end

      def process(data)
        node = Dataflow::Nodes::ComputeNode.find(data['node_id'])

        unless node.execution_valid?(data['execution_uuid'])
          logger.log("[#{data['msg_id']}] work on '#{node.name}' has expired. Skipping.")
          return
        end

        results = execute(node, data)
        response = { msg_id: data['msg_id'] }
        response.merge(results[0])
      rescue Mongoid::Errors::DocumentNotFound => e
        { error: { message: e.message, backtrace: e.backtrace } }
      end

      def execute(node, payload_data)
        # execute in a different process, so that once it's finished
        # we can purge the memory
        Parallel.map([payload_data]) do |data|
          result = {}
          logger.log("[#{data['msg_id']}] working on '#{node.name}'...")

          begin
            if data['is_batch']
              records = node.execute_local_batch_computation(data['params'])
              # in ruby, we already have access to the node, so we
              # add the data directly here instead of returning it through
              # the queue. The default batch behavior on other languages
              # is to return the output data in the 'data' key, e.g.:
              # result['data] = records
              node.data_node&.add(records: records)
            else
              node.execute_local_computation
            end
          rescue StandardError => e
            result = { error: { message: e.message, backtrace: e.backtrace } }
          end

          logger.log("[#{data['msg_id']}] done working on '#{node.name}'.")
          result
        end
      end

      def logger
        @logger ||= Dataflow::Logger.new(prefix: 'Worker')
      end
    end
  end
end
