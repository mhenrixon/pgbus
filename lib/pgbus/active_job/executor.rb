# frozen_string_literal: true

module Pgbus
  module ActiveJob
    class Executor
      attr_reader :client, :config

      def initialize(client: Pgbus.client, config: Pgbus.configuration)
        @client = client
        @config = config
      end

      def execute(message, queue_name, source_queue: nil)
        payload = JSON.parse(message.message)
        read_count = message.read_ct.to_i

        if read_count > config.max_retries
          handle_dead_letter(message, queue_name, payload, source_queue: source_queue)
          signal_concurrency(payload)
          signal_batch_discarded(payload)
          Uniqueness.release_lock(Uniqueness.extract_key(payload))
          return :dead_lettered
        end

        job_class = payload["job_class"]
        uniqueness_key = Uniqueness.extract_key(payload)

        # For :while_executing strategy, acquire the lock now (at execution time).
        # If another worker is already executing this job, skip it — VT will expire
        # and it'll be retried later when the lock is released.
        if uniqueness_key && Uniqueness.extract_strategy(payload) == :while_executing && !Uniqueness.acquire_execution_lock(uniqueness_key,
                                                                                                                            payload)
          Pgbus.logger.info { "[Pgbus] Skipping duplicate execution for #{job_class}: #{uniqueness_key}" }
          return :skipped
        end

        job_succeeded = false

        Instrumentation.instrument("pgbus.executor.execute", queue: queue_name, job_class: job_class) do
          job = ::ActiveJob::Base.deserialize(payload)
          execute_job(job)
          archive_from(queue_name, message.msg_id.to_i, source_queue: source_queue)
          job_succeeded = true
        end

        instrument("pgbus.job_completed", queue: queue_name, job_class: job_class)
        :success
      rescue StandardError => e
        handle_failure(message, queue_name, e)
        instrument("pgbus.job_failed", queue: queue_name, job_class: payload&.dig("job_class"), error: e.class.name)
        # Don't signal concurrency on transient failure — the job will be retried.
        # Semaphore is released only on success or dead-lettering.
        :failed
      ensure
        # Signal concurrency and batch only when the job was archived successfully.
        # job_succeeded is set AFTER archive_message, so if archive fails the
        # semaphore slot stays held until VT expires and the job is retried.
        if job_succeeded
          signal_concurrency(payload)
          signal_batch_completed(payload)
          # Release uniqueness lock on successful completion (both strategies)
          Uniqueness.release_lock(uniqueness_key) if uniqueness_key
        end
      end

      private

      def execute_job(job)
        if defined?(Rails) && Rails.application
          Rails.application.executor.wrap { job.perform_now }
        else
          job.perform_now
        end
      end

      def handle_failure(_message, _queue_name, error)
        Pgbus.logger.error { "[Pgbus] Job failed: #{error.class}: #{error.message}" }
        Pgbus.logger.debug { error.backtrace&.join("\n") }

        # Message visibility timeout will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ.
      end

      def instrument(event_name, payload = {})
        return unless defined?(ActiveSupport::Notifications)

        ActiveSupport::Notifications.instrument(event_name, payload)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Notification failure #{event_name}: #{e.class}: #{e.message}" }
      end

      def signal_concurrency(payload)
        key = Concurrency.extract_key(payload)
        return unless key

        # Atomic permit handoff: try to promote a blocked job first.
        # promote_next wraps delete + enqueue in a transaction so neither is lost.
        # If promoted, the slot stays occupied (no release needed).
        # Only release the semaphore if there's nothing to promote.
        promoted = Concurrency::BlockedExecution.promote_next(key, client: client)
        Concurrency::Semaphore.release(key) unless promoted
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Concurrency signal failed: #{e.message}" }
      end

      def signal_batch_completed(payload)
        batch_id = payload[Batch::METADATA_KEY]
        return unless batch_id

        Batch.job_completed(batch_id)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Batch completion signal failed: #{e.message}" }
      end

      def signal_batch_discarded(payload)
        batch_id = payload[Batch::METADATA_KEY]
        return unless batch_id

        Batch.job_discarded(batch_id)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Batch discard signal failed: #{e.message}" }
      end

      def archive_from(queue_name, msg_id, source_queue: nil)
        if source_queue
          client.archive_from_queue(source_queue, msg_id)
        else
          client.archive_message(queue_name, msg_id)
        end
      end

      def handle_dead_letter(message, queue_name, payload, source_queue: nil)
        Pgbus.logger.warn do
          job_class = payload["job_class"] || "unknown"
          "[Pgbus] Moving job #{job_class} to dead letter queue after #{message.read_ct} attempts"
        end
        if source_queue
          client.ensure_dead_letter_queue(queue_name)
          dlq_name = config.dead_letter_queue_name(queue_name)
          client.transaction do |txn|
            txn.produce(dlq_name, message.message, headers: message.headers)
            txn.delete(source_queue, message.msg_id.to_i)
          end
        else
          client.move_to_dead_letter(queue_name, message)
        end
      end
    end
  end
end
