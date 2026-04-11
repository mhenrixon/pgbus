# frozen_string_literal: true

require "time"

module Pgbus
  module ActiveJob
    class Executor
      attr_reader :client, :config

      def initialize(client: Pgbus.client, config: Pgbus.configuration, stat_buffer: nil)
        @client = client
        @config = config
        @stat_buffer = stat_buffer
      end

      def execute(message, queue_name, source_queue: nil)
        execution_start = monotonic_now
        payload = JSON.parse(message.message)
        read_count = message.read_ct.to_i

        if read_count > config.max_retries
          handle_dead_letter(message, queue_name, payload, source_queue: source_queue)
          FailedEventRecorder.clear!(queue_name: queue_name, msg_id: message.msg_id.to_i)
          signal_concurrency(payload)
          signal_batch_discarded(payload)
          Uniqueness.release_lock(Uniqueness.extract_key(payload))
          record_stat(payload, queue_name, "dead_lettered", execution_start, message: message)
          return :dead_lettered
        end

        job_class = payload["job_class"]
        uniqueness_key = Uniqueness.extract_key(payload)
        uniqueness_strategy = Uniqueness.extract_strategy(payload)

        if uniqueness_key
          case uniqueness_strategy
          when :until_executed
            # No claim step needed — PGMQ's visibility timeout is the execution lock.
            # The uniqueness key row was inserted at enqueue time and will be
            # released on completion or DLQ.
            nil
          when :while_executing
            # Acquire the lock now. If another worker is already executing
            # this job, skip it — VT will expire and it'll be retried.
            unless Uniqueness.acquire_execution_lock(uniqueness_key, payload)
              Pgbus.logger.info { "[Pgbus] Skipping duplicate execution for #{job_class}" }
              return :skipped
            end
          end
        end

        job_succeeded = false

        Instrumentation.instrument("pgbus.executor.execute", queue: queue_name, job_class: job_class) do
          job = ::ActiveJob::Base.deserialize(payload)
          execute_job(job)
          archive_from(queue_name, message.msg_id.to_i, source_queue: source_queue)
          FailedEventRecorder.clear!(queue_name: queue_name, msg_id: message.msg_id.to_i)
          job_succeeded = true
        end

        instrument("pgbus.job_completed", queue: queue_name, job_class: job_class)
        record_stat(payload, queue_name, "success", execution_start, message: message)
        :success
      rescue StandardError => e
        handle_failure(message, queue_name, e, payload: payload)
        instrument("pgbus.job_failed", queue: queue_name, job_class: payload&.dig("job_class"), error: e.class.name)
        record_stat(payload, queue_name, "failed", execution_start, message: message)
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
        if defined?(Rails) && Rails.respond_to?(:application) && Rails.application
          Rails.application.executor.wrap { job.perform_now }
        else
          job.perform_now
        end
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      def record_stat(payload, queue_name, status, start_time, message: nil)
        return unless config.stats_enabled

        attrs = {
          job_class: payload&.dig("job_class") || "unknown",
          queue_name: queue_name,
          status: status,
          duration_ms: ((monotonic_now - start_time) * 1000).round,
          enqueue_latency_ms: compute_enqueue_latency(message),
          retry_count: message ? [message.read_ct.to_i - 1, 0].max : 0
        }

        if @stat_buffer
          @stat_buffer.push(attrs)
        else
          JobStat.record!(**attrs)
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Stat recording failed: #{e.message}" }
      end

      def compute_enqueue_latency(message)
        return unless message

        enqueued_at = message.enqueued_at
        return unless enqueued_at

        # Fast path: numeric epoch (float seconds) avoids Time.parse entirely.
        # PGMQ returns enqueued_at as a Time or string depending on the driver.
        case enqueued_at
        when Numeric
          [((Time.now.to_f - enqueued_at) * 1000).round, 0].max
        when Time
          [((Time.now.utc - enqueued_at.utc) * 1000).round, 0].max
        else
          parse_enqueue_latency_from_string(enqueued_at.to_s)
        end
      rescue ArgumentError, TypeError
        nil
      end

      def parse_enqueue_latency_from_string(str)
        # PGMQ enqueued_at is TIMESTAMPTZ (always UTC internally).
        # If the string lacks an explicit offset, assume UTC to avoid
        # misinterpretation when the system timezone is non-UTC.
        str = "#{str} UTC" unless str.match?(/[+-]\d{2}:?\d{2}\s*$|Z\s*$/i)
        enqueued_at = Time.parse(str)
        [((Time.now.utc - enqueued_at) * 1000).round, 0].max
      end

      def handle_failure(message, queue_name, error, payload: nil)
        ctx = { queue: queue_name, job_class: payload&.dig("job_class"),
                msg_id: message.msg_id.to_i, read_ct: message.read_ct.to_i }
        ErrorReporter.report(error, ctx)
        Pgbus.logger.debug { error.backtrace&.join("\n") }

        # Record failure for dashboard visibility.
        # Message visibility timeout will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ.
        FailedEventRecorder.record!(
          queue_name: queue_name,
          msg_id: message.msg_id.to_i,
          payload: payload || message.message,
          headers: message.respond_to?(:headers) ? message.headers : nil,
          error: error,
          retry_count: [message.read_ct.to_i - 1, 0].max
        )

        apply_retry_backoff(message, queue_name, payload)
      end

      # Extend the message's visibility timeout with exponential backoff
      # so retries aren't all bunched at the default flat VT interval.
      # Skipped on the first read (read_ct=1) — that's the initial
      # attempt, not a retry.
      def apply_retry_backoff(message, queue_name, payload)
        attempt = message.read_ct.to_i - 1
        return if attempt < 1

        job_class = resolve_job_class(payload)
        delay = if job_class
                  RetryBackoff.compute_delay_for_job(job_class, attempt: attempt)
                else
                  RetryBackoff.compute_delay(attempt: attempt)
                end

        client.set_visibility_timeout(queue_name, message.msg_id.to_i, vt: delay)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Retry backoff VT update failed: #{e.message}" }
      end

      def resolve_job_class(payload)
        return unless payload.is_a?(Hash) && payload["job_class"]

        payload["job_class"].constantize
      rescue NameError
        nil
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
          client.archive_message(source_queue, msg_id, prefixed: false)
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
