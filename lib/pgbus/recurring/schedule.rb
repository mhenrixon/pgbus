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

        RecurringExecutionRecord.record(task.key, run_at) do
          payload = build_payload(task)
          headers = build_headers(task, run_at)

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
        @config.queue_name(task.queue_name || @config.default_queue)
      end

      def build_headers(task, run_at)
        {
          "pgbus.recurring_key" => task.key,
          "pgbus.recurring_run_at" => run_at.iso8601,
          "pgbus.recurring_schedule" => task.schedule
        }
      end
    end
  end
end
