# frozen_string_literal: true

require "active_job"

module Pgbus
  module ActiveJob
    class Adapter
      def enqueue(active_job)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = JSON.parse(Serializer.serialize_job(active_job))
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)

        enqueue_with_concurrency(active_job, queue, payload_hash)
      end

      def enqueue_at(active_job, timestamp)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = JSON.parse(Serializer.serialize_job(active_job))
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        delay = [(timestamp - Time.now.to_f).ceil, 0].max

        enqueue_with_concurrency(active_job, queue, payload_hash, delay: delay)
      end

      def enqueue_all(active_jobs)
        active_jobs.group_by { |j| j.queue_name || Pgbus.configuration.default_queue }.each do |queue, jobs|
          enqueue_immediate(queue, jobs.reject { |j| j.scheduled_at && j.scheduled_at > Time.now })
          jobs.select { |j| j.scheduled_at && j.scheduled_at > Time.now }.each { |j| enqueue_at(j, j.scheduled_at.to_f) }
        end

        active_jobs.count
      end

      private

      def enqueue_with_concurrency(active_job, queue, payload_hash, delay: 0)
        key = Concurrency.extract_key(payload_hash)
        concurrency = concurrency_config(active_job)

        if key && concurrency
          result = Concurrency::Semaphore.acquire(key, concurrency[:limit], concurrency[:duration])

          if result == :acquired
            msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay)
            active_job.provider_job_id = msg_id
          else
            handle_conflict(concurrency, active_job, key, queue, payload_hash)
          end
        else
          msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay)
          active_job.provider_job_id = msg_id
        end

        active_job
      end

      def concurrency_config(active_job)
        active_job.class.respond_to?(:pgbus_concurrency) && active_job.class.pgbus_concurrency
      end

      def handle_conflict(concurrency, active_job, key, queue, payload_hash)
        case concurrency[:on_conflict]
        when :block
          Concurrency::BlockedExecution.insert(
            concurrency_key: key,
            queue_name: queue,
            payload: payload_hash,
            priority: active_job.try(:priority) || 0,
            duration: concurrency[:duration]
          )
        when :discard
          Pgbus.logger.info { "[Pgbus] Discarding job #{active_job.class.name}: concurrency limit for #{key}" }
        when :raise
          raise ConcurrencyLimitExceeded, "Concurrency limit reached for key: #{key}"
        end
      end

      def enqueue_immediate(queue, jobs)
        return if jobs.empty?

        payloads = jobs.map { |j| JSON.parse(Serializer.serialize_job(j)) }
        msg_ids = Pgbus.client.send_batch(queue, payloads)

        unless msg_ids.is_a?(Array) && msg_ids.size == jobs.size
          raise "Pgbus batch enqueue failed: expected #{jobs.size} ids, got #{msg_ids&.size || 0}"
        end

        jobs.zip(msg_ids).each { |job, id| job.provider_job_id = id }
      end

      def enqueue_after_transaction_commit?
        true
      end
    end
  end
end

# Register the adapter with ActiveJob (register method added in Rails 7.2+)
ActiveJob::QueueAdapters.register(:pgbus, Pgbus::ActiveJob::Adapter) if ActiveJob::QueueAdapters.respond_to?(:register)
