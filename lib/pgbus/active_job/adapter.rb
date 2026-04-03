# frozen_string_literal: true

require "active_job"

module Pgbus
  module ActiveJob
    class Adapter
      def enqueue(active_job)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = Serializer.serialize_job_hash(active_job)
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        payload_hash = Uniqueness.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash)

        return active_job if uniqueness_rejected?(active_job, payload_hash)

        enqueue_with_concurrency(active_job, queue, payload_hash)
      end

      def enqueue_at(active_job, timestamp)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = Serializer.serialize_job_hash(active_job)
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        payload_hash = Uniqueness.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash)
        delay = [(timestamp - Time.current.to_f).ceil, 0].max

        return active_job if uniqueness_rejected?(active_job, payload_hash)

        enqueue_with_concurrency(active_job, queue, payload_hash, delay: delay)
      end

      def enqueue_all(active_jobs)
        # Jobs with uniqueness must go through individual enqueue to acquire locks
        unique, bulk = active_jobs.partition { |j| Uniqueness.uniqueness_config(j) }
        unique.each do |j|
          if scheduled_in_future?(j)
            enqueue_at(j, j.scheduled_at.to_f)
          else
            enqueue(j)
          end
        end

        bulk.group_by { |j| j.queue_name || Pgbus.configuration.default_queue }.each do |queue, jobs|
          enqueue_immediate(queue, jobs.reject { |j| scheduled_in_future?(j) })
          jobs.select { |j| scheduled_in_future?(j) }.each { |j| enqueue_at(j, j.scheduled_at.to_f) }
        end

        active_jobs.count
      end

      private

      def enqueue_with_concurrency(active_job, queue, payload_hash, delay: 0)
        key = Concurrency.extract_key(payload_hash)
        concurrency = concurrency_config(active_job)
        priority = active_job.try(:priority)

        if key && concurrency
          result = Concurrency::Semaphore.acquire(key, concurrency[:limit], concurrency[:duration])

          if result == :acquired
            msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
            active_job.provider_job_id = msg_id
          else
            handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: priority)
          end
        else
          msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
          active_job.provider_job_id = msg_id
        end

        Thread.current[:pgbus_acquired_uniqueness_key] = nil
        active_job
      rescue StandardError => e
        # Roll back the uniqueness lock if enqueue failed
        rollback_key = Thread.current[:pgbus_acquired_uniqueness_key]
        if rollback_key
          begin
            Uniqueness.release_lock(rollback_key)
          rescue StandardError => rollback_error
            Pgbus.logger.warn { "[Pgbus] Lock rollback failed: #{rollback_error.message}" }
          end
          Thread.current[:pgbus_acquired_uniqueness_key] = nil
        end
        raise e
      end

      def concurrency_config(active_job)
        active_job.class.respond_to?(:pgbus_concurrency) && active_job.class.pgbus_concurrency
      end

      def handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: nil)
        case concurrency[:on_conflict]
        when :block
          Concurrency::BlockedExecution.insert(
            concurrency_key: key,
            queue_name: queue,
            payload: payload_hash,
            priority: priority || Pgbus.configuration.default_priority,
            duration: concurrency[:duration]
          )
        when :discard
          Pgbus.logger.info { "[Pgbus] Discarding job #{active_job.class.name}: concurrency limit for #{key}" }
        when :raise
          raise ConcurrencyLimitExceeded, "Concurrency limit reached for key: #{key}"
        end
      end

      def uniqueness_rejected?(active_job, payload_hash)
        uniqueness_key = Uniqueness.extract_key(payload_hash)
        return false unless uniqueness_key

        result = Uniqueness.acquire_enqueue_lock(uniqueness_key, active_job)

        # :no_lock means no enqueue-time lock needed (e.g. :while_executing strategy)
        return false if result == :no_lock

        # Store the acquired key so we can release it if enqueue fails
        Thread.current[:pgbus_acquired_uniqueness_key] = uniqueness_key if result == :acquired
        return false if result == :acquired

        config = Uniqueness.uniqueness_config(active_job)
        case config[:on_conflict]
        when :reject
          raise JobNotUnique, "Job #{active_job.class.name} is already locked"
        when :discard
          Pgbus.logger.info { "[Pgbus] Discarding duplicate job #{active_job.class.name}" }
          true
        when :log
          Pgbus.logger.warn { "[Pgbus] Duplicate job #{active_job.class.name} detected" }
          true
        else
          true
        end
      end

      def inject_batch_metadata(payload_hash)
        batch_id = Thread.current[:pgbus_batch_id]
        return payload_hash unless batch_id

        Thread.current[:pgbus_batch_job_count] = (Thread.current[:pgbus_batch_job_count] || 0) + 1
        payload_hash.merge(Batch::METADATA_KEY => batch_id)
      end

      def enqueue_immediate(queue, jobs)
        return if jobs.empty?

        payloads = jobs.map { |j| Serializer.serialize_job_hash(j) }
        msg_ids = Pgbus.client.send_batch(queue, payloads)

        unless msg_ids.is_a?(Array) && msg_ids.size == jobs.size
          raise "Pgbus batch enqueue failed: expected #{jobs.size} ids, got #{msg_ids&.size || 0}"
        end

        jobs.zip(msg_ids).each { |job, id| job.provider_job_id = id }
      rescue Pgbus::SchemaNotReady => e
        Pgbus.logger.error { "[Pgbus] #{e.message}" }
        raise
      end

      def scheduled_in_future?(job)
        job.scheduled_at && job.scheduled_at > Time.current
      end

      def enqueue_after_transaction_commit?
        true
      end
    end
  end
end
