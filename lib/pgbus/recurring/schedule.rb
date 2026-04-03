# frozen_string_literal: true

module Pgbus
  module Recurring
    class Schedule
      attr_reader :tasks

      def initialize(config: Pgbus.configuration)
        @config = config
        @tasks = load_tasks
      end

      def due_tasks(time = Time.now)
        tasks.select { |task| task_due?(task, time) }
      end

      def enqueue_task(task, run_at:)
        queue = resolve_queue(task)

        # Check uniqueness lock before enqueuing. If the job class declares
        # ensures_uniqueness, we acquire the lock here so duplicate recurring
        # enqueues are rejected while a previous instance is still queued or running.
        if uniqueness_locked?(task)
          Pgbus.logger.debug do
            "[Pgbus] Recurring task #{task.key} skipped: uniqueness lock held"
          end
          return
        end

        RecurringExecution.record(task.key, run_at) do
          payload = build_payload(task)
          headers = build_headers(task, run_at)

          # Inject uniqueness metadata into the payload so the worker knows
          # to release the lock after execution.
          payload = inject_uniqueness_metadata(task, payload)

          Pgbus.client.ensure_queue(queue)
          Pgbus.client.send_message(queue, payload, headers: headers)

          Pgbus.logger.info do
            "[Pgbus] Enqueued recurring task #{task.key} (#{task.class_name || task.command}) " \
              "for run_at=#{run_at.iso8601}"
          end
        end
      rescue AlreadyRecorded
        Pgbus.logger.debug { "[Pgbus] Recurring task #{task.key} already enqueued for #{run_at.iso8601}" }
      end

      def build_payload(task)
        if task.command
          {
            "job_class" => "Pgbus::Recurring::CommandJob",
            "arguments" => [task.command],
            "queue_name" => task.queue_name || @config.default_queue,
            "priority" => nil
          }
        else
          {
            "job_class" => task.class_name,
            "arguments" => task.arguments,
            "queue_name" => task.queue_name || @config.default_queue,
            "priority" => task.priority.zero? ? nil : task.priority
          }
        end
      end

      private

      def load_tasks
        raw = @config.recurring_tasks || {}
        raw.filter_map do |key, options|
          options = options.transform_keys(&:to_s).transform_keys(&:to_sym) if options.is_a?(Hash)
          task = Task.from_configuration(key, **(options || {}))
          if task.valid?
            task
          else
            Pgbus.logger.warn { "[Pgbus] Skipping invalid recurring task '#{key}': #{task.errors.join(", ")}" }
            nil
          end
        end
      end

      def task_due?(task, time)
        # A task is due when its most recent cron occurrence (previous_time)
        # falls within the current tick window. We also check match? to
        # handle the exact-boundary case where time == cron time.
        cron = task.parsed_schedule
        return false unless cron

        # Check if `time` itself matches the cron (exact boundary hit)
        return true if cron.match?(time)

        # Check if the previous occurrence was recent enough that we should
        # still fire it (handles the case where we tick slightly after the
        # cron time). The window is the scheduler interval.
        prev = task.previous_time(time)
        return false unless prev

        (time - prev) <= @config.recurring_schedule_interval
      end

      def resolve_queue(task)
        task.queue_name || @config.default_queue
      end

      def build_headers(task, run_at)
        {
          "pgbus.recurring_key" => task.key,
          "pgbus.recurring_run_at" => run_at.iso8601,
          "pgbus.recurring_schedule" => task.schedule
        }
      end

      # Check if the job class has ensures_uniqueness and if its lock is currently held.
      # Returns true if the lock is held (skip enqueue), false otherwise.
      def uniqueness_locked?(task)
        return false unless task.class_name

        job_class = task.class_name.safe_constantize
        return false unless job_class
        return false unless job_class.respond_to?(:pgbus_uniqueness)

        config = job_class.pgbus_uniqueness
        return false unless config
        return false unless config[:strategy] == :until_executed

        key = resolve_uniqueness_key(job_class, config, task)
        return false unless key

        # Try to acquire the lock. If it fails, the lock is already held.
        acquired = JobLock.acquire!(
          key,
          job_class: task.class_name,
          job_id: "recurring-#{task.key}",
          state: "queued",
          ttl: config[:lock_ttl]
        )
        # If we acquired it, great — the message will be enqueued with the lock held.
        # If not, a previous instance is still queued/running.
        !acquired
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness check failed for #{task.key}: #{e.message}" }
        false # Fail open — allow enqueue if uniqueness check errors
      end

      # Resolve the uniqueness key for a recurring task.
      # For no-argument recurring jobs, the key defaults to the class name.
      def resolve_uniqueness_key(job_class, config, task)
        key_proc = config[:key]
        args = task.arguments || []

        if args.empty?
          key_proc.call
        else
          key_proc.call(*args)
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Could not resolve uniqueness key for #{task.key}: #{e.message}" }
        nil
      end

      # Inject uniqueness metadata into the payload so the executor releases
      # the lock after the job completes.
      def inject_uniqueness_metadata(task, payload)
        return payload unless task.class_name

        job_class = task.class_name.safe_constantize
        return payload unless job_class&.respond_to?(:pgbus_uniqueness)

        config = job_class.pgbus_uniqueness
        return payload unless config

        key = resolve_uniqueness_key(job_class, config, task)
        return payload unless key

        payload.merge(
          Pgbus::Uniqueness::METADATA_KEY => key,
          Pgbus::Uniqueness::STRATEGY_KEY => config[:strategy].to_s,
          Pgbus::Uniqueness::TTL_KEY => config[:lock_ttl]
        )
      end
    end
  end
end
