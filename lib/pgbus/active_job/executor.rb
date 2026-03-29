# frozen_string_literal: true

module Pgbus
  module ActiveJob
    class Executor
      attr_reader :client, :config

      def initialize(client: Pgbus.client, config: Pgbus.configuration)
        @client = client
        @config = config
      end

      def execute(message, queue_name)
        payload = JSON.parse(message.message)
        read_count = message.read_ct.to_i

        if read_count > config.max_retries
          handle_dead_letter(message, queue_name, payload)
          return :dead_lettered
        end

        job = ::ActiveJob::Base.deserialize(payload)
        execute_job(job)
        client.archive_message(queue_name, message.msg_id.to_i)
        :success
      rescue StandardError => e
        handle_failure(message, queue_name, e)
        :failed
      end

      private

      def execute_job(job)
        if defined?(Rails) && Rails.application
          Rails.application.executor.wrap { job.perform_now }
        else
          job.perform_now
        end
      end

      def handle_failure(message, queue_name, error)
        Pgbus.logger.error { "[Pgbus] Job failed: #{error.class}: #{error.message}" }
        Pgbus.logger.debug { error.backtrace&.join("\n") }

        # Message visibility timeout will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ.
      end

      def handle_dead_letter(message, queue_name, payload)
        Pgbus.logger.warn do
          job_class = payload["job_class"] || "unknown"
          "[Pgbus] Moving job #{job_class} to dead letter queue after #{message.read_ct} attempts"
        end
        client.move_to_dead_letter(queue_name, message)
      end
    end
  end
end
