# frozen_string_literal: true

require "active_job"

module Pgbus
  module ActiveJob
    class Adapter
      def enqueue(active_job)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload = Serializer.serialize_job(active_job)
        msg_id = Pgbus.client.send_message(queue, JSON.parse(payload))
        active_job.provider_job_id = msg_id
        active_job
      end

      def enqueue_at(active_job, timestamp)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload = Serializer.serialize_job(active_job)
        delay = [(timestamp - Time.now.to_f).ceil, 0].max
        msg_id = Pgbus.client.send_message(queue, JSON.parse(payload), delay: delay)
        active_job.provider_job_id = msg_id
        active_job
      end

      def enqueue_all(active_jobs)
        jobs_by_queue = active_jobs.group_by { |j| j.queue_name || Pgbus.configuration.default_queue }

        jobs_by_queue.each do |queue, jobs|
          scheduled, immediate = jobs.partition { |j| j.scheduled_at && j.scheduled_at > Time.now }

          if immediate.any?
            payloads = immediate.map { |j| JSON.parse(Serializer.serialize_job(j)) }
            msg_ids = Pgbus.client.send_batch(queue, payloads)
            immediate.zip(msg_ids).each { |job, id| job.provider_job_id = id }
          end

          scheduled.each { |j| enqueue_at(j, j.scheduled_at.to_f) }
        end

        active_jobs.count
      end

      def enqueue_after_transaction_commit?
        true
      end
    end
  end
end

# Register the adapter with ActiveJob
ActiveJob::QueueAdapters.register(:pgbus, Pgbus::ActiveJob::Adapter)
