# frozen_string_literal: true

require "time"

module Pgbus
  module Web
    class DataSource
      def initialize(client: Pgbus.client)
        @client = client
        @last_throughput_snapshot = nil
        @last_throughput_at = nil
      end

      # Dashboard summary
      def summary_stats
        queues = queues_with_metrics
        total_depth = queues.sum { |q| q[:queue_length] }
        total_visible = queues.sum { |q| q[:queue_visible_length] }
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        dlq_depth = queues.select { |q| q[:name].end_with?(dlq_suffix) }.sum { |q| q[:queue_length] }

        throughput = compute_throughput(queues)

        {
          total_queues: queues.size,
          total_depth: total_depth,
          total_visible: total_visible,
          active_processes: processes.count,
          failed_count: failed_events_count,
          dlq_depth: dlq_depth,
          recurring_count: recurring_tasks_count,
          throughput_rate: throughput
        }
      end

      # Queues — query via ActiveRecord for reliability in web processes
      # (avoids PGMQ client connection issues when the web server uses a
      # different connection lifecycle than the worker processes).
      def queues_with_metrics
        queue_names = connection.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        paused_queues = paused_queue_names
        queue_names.map { |name| queue_metrics_via_sql(name) }.compact.map do |q|
          q.merge(paused: paused_queues.include?(logical_queue_name(q[:name])))
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching queue metrics: #{e.class}: #{e.message}" }
        []
      end

      # name is the full PGMQ queue name (e.g. "pgbus_default") as returned
      # by queues_with_metrics. No prefix is added.
      def queue_detail(name)
        queue_metrics_via_sql(name)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching queue detail for #{name}: #{e.class}: #{e.message}" }
        nil
      end

      def purge_queue(name)
        release_uniqueness_keys_for_queue(name)
        @client.purge_queue(name, prefixed: false)
      end

      def drop_queue(name)
        release_uniqueness_keys_for_queue(name)
        @client.drop_queue(name, prefixed: false)
      end

      def pause_queue(name, reason: nil)
        QueueState.pause!(logical_queue_name(name), reason: reason)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error pausing queue #{name}: #{e.message}" }
      end

      def resume_queue(name)
        QueueState.resume!(logical_queue_name(name))
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error resuming queue #{name}: #{e.message}" }
      end

      def queue_paused?(name)
        QueueState.paused?(logical_queue_name(name))
      rescue StandardError
        false
      end

      # Jobs (messages in queue tables)
      def jobs(queue_name: nil, page: 1, per_page: 25)
        offset = (page - 1) * per_page

        if queue_name
          query_queue_messages(queue_name, per_page, offset)
        else
          all_queue_messages(per_page, offset)
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error reading jobs: #{e.message}" }
        []
      end

      def job_detail(queue_name, msg_id)
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus Job Detail",
          [msg_id.to_i]
        )
        row ? format_message(row, queue_name) : nil
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching job detail: #{e.message}" }
        nil
      end

      def retry_job(queue_name, msg_id)
        @client.set_visibility_timeout(queue_name, msg_id.to_i, vt: 0, prefixed: false)
      end

      def discard_job(queue_name, msg_id)
        release_lock_for_message(queue_name, msg_id)
        @client.archive_message(queue_name, msg_id.to_i, prefixed: false)
      end

      def discard_all_enqueued
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        queues = queues_with_metrics.reject { |q| q[:name].end_with?(dlq_suffix) }
        total = 0

        queues.each do |q|
          messages = query_queue_messages_raw(q[:name], 10_000, 0)
          next if messages.empty?

          release_locks_for_messages(messages)

          ids = messages.map { |m| m[:msg_id].to_i }
          @client.archive_batch(q[:name], ids, prefixed: false)
          total += ids.size
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error discarding enqueued messages from #{q[:name]}: #{e.message}" }
        end

        total
      end

      # Failed events
      def failed_events(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        rows = connection.select_all(
          "SELECT * FROM pgbus_failed_events ORDER BY failed_at DESC LIMIT $1 OFFSET $2",
          "Pgbus Failed Events",
          [per_page, offset]
        )
        rows.to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching failed events: #{e.message}" }
        []
      end

      def failed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_failed_events")
        result.to_i
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting failed events: #{e.message}" }
        0
      end

      def failed_event(id)
        connection.select_one(
          "SELECT * FROM pgbus_failed_events WHERE id = $1",
          "Pgbus Failed Event",
          [id.to_i]
        )
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching failed event #{id}: #{e.message}" }
        nil
      end

      def retry_failed_event(id)
        event = failed_event(id)
        return false unless event

        # Prefer resetting the existing message's visibility timeout to 0
        # so the worker picks it up immediately. This avoids creating a
        # duplicate (the original is still in the queue waiting for retry).
        # Falls back to enqueueing a fresh copy only if the original is gone
        # (e.g., already moved to DLQ).
        msg_id = event["msg_id"]
        if msg_id && @client.message_exists?(event["queue_name"], msg_id: msg_id.to_i)
          @client.set_visibility_timeout(event["queue_name"], msg_id.to_i, vt: 0)
        else
          payload = JSON.parse(event["payload"])
          headers = event["headers"]
          headers = JSON.parse(headers) if headers.is_a?(String)
          @client.send_message(event["queue_name"], payload, headers: headers)
        end

        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [id.to_i]
        )
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error retrying failed event #{id}: #{e.message}" }
        false
      end

      def discard_failed_event(id)
        event = failed_event(id)
        if event
          release_lock_for_payload(event["payload"])
          archive_failed_message(event)
        end

        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [id.to_i]
        )
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding failed event #{id}: #{e.message}" }
        false
      end

      def retry_all_failed
        count = 0
        loop do
          batch = connection.select_all(
            "SELECT * FROM pgbus_failed_events ORDER BY id LIMIT 100", "Pgbus Retry Batch"
          ).to_a
          break if batch.empty?

          batch.each do |event|
            payload = JSON.parse(event["payload"])
            headers = event["headers"]
            headers = JSON.parse(headers) if headers.is_a?(String)

            connection.transaction do
              @client.send_message(event["queue_name"], payload, headers: headers)
              connection.exec_delete(
                "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [event["id"].to_i]
              )
            end
            count += 1
          rescue StandardError => e
            Pgbus.logger.error { "[Pgbus::Web] Failed to retry event #{event["id"]}: #{e.message}" }
          end
        end
        count
      end

      def discard_all_failed
        release_locks_for_failed_events
        archive_all_failed_messages

        result = connection.execute("DELETE FROM pgbus_failed_events")
        result.cmd_tuples
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding all failed events: #{e.message}" }
        0
      end

      # Dead letter queue
      # Note: DLQ queue names from queues_with_metrics are already fully qualified
      # (e.g., "pgbus_default_dlq"), so we use them directly without re-prefixing.
      def dlq_messages(page: 1, per_page: 25)
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        queues = queues_with_metrics.select { |q| q[:name].end_with?(dlq_suffix) }
        offset = (page - 1) * per_page

        messages = queues.flat_map do |q|
          query_queue_messages_raw(q[:name], per_page + offset, 0)
        end

        messages.sort_by { |m| -m[:msg_id].to_i }.slice(offset, per_page) || []
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ messages: #{e.message}" }
        []
      end

      def dlq_message_detail(msg_id)
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        queues = queues_with_metrics.select { |q| q[:name].end_with?(dlq_suffix) }
        queues.each do |q|
          row = connection.select_one(
            "SELECT * FROM pgmq.q_#{sanitize_name(q[:name])} WHERE msg_id = $1",
            "Pgbus DLQ Detail",
            [msg_id.to_i]
          )
          return format_message(row, q[:name]) if row
        end
        nil
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ message #{msg_id}: #{e.message}" }
        nil
      end

      def retry_dlq_message(queue_name, msg_id)
        # queue_name here is the full DLQ name (already prefixed)
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        original_queue = queue_name.delete_suffix(dlq_suffix)

        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus DLQ Read",
          [msg_id.to_i]
        )
        return false unless row

        @client.transaction do |txn|
          txn.produce(original_queue, row["message"], headers: row["headers"])
          txn.delete(queue_name, msg_id.to_i)
        end
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error retrying DLQ message #{msg_id}: #{e.message}" }
        false
      end

      def discard_dlq_message(queue_name, msg_id)
        # queue_name here is the full DLQ name (already prefixed)
        release_lock_for_message(queue_name, msg_id)
        @client.delete_message(queue_name, msg_id.to_i, prefixed: false)
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding DLQ message #{msg_id}: #{e.message}" }
        false
      end

      def retry_all_dlq
        messages = dlq_messages(page: 1, per_page: 1000)
        count = 0
        messages.each do |m|
          retry_dlq_message(m[:queue_name], m[:msg_id]) && count += 1
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error retrying DLQ message #{m[:msg_id]}: #{e.message}" }
          next
        end
        count
      end

      def discard_all_dlq
        messages = dlq_messages(page: 1, per_page: 1000)
        return 0 if messages.empty?

        release_locks_for_messages(messages)

        # Group by queue for batch delete — one call per DLQ instead of N calls
        messages.group_by { |m| m[:queue_name] }.sum do |queue_name, msgs|
          ids = msgs.map { |m| m[:msg_id].to_i }
          @client.delete_batch(queue_name, ids, prefixed: false).size
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error batch-discarding DLQ messages from #{queue_name}: #{e.message}" }
          0
        end
      end

      # Processes
      def processes
        rows = connection.select_all(
          "SELECT * FROM pgbus_processes ORDER BY kind, created_at"
        )
        rows.to_a.map { |r| format_process(r) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processes: #{e.message}" }
        []
      end

      # Processed events (audit trail)
      def processed_events(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        rows = connection.select_all(
          "SELECT * FROM pgbus_processed_events ORDER BY processed_at DESC LIMIT $1 OFFSET $2",
          "Pgbus Processed Events",
          [per_page, offset]
        )
        rows.to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processed events: #{e.message}" }
        []
      end

      def processed_event(id)
        connection.select_one(
          "SELECT * FROM pgbus_processed_events WHERE id = $1",
          "Pgbus Processed Event",
          [id.to_i]
        )
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processed event #{id}: #{e.message}" }
        nil
      end

      def processed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_processed_events")
        result.to_i
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting processed events: #{e.message}" }
        0
      end

      def replay_event(event)
        # Re-publish the event payload to all matching subscribers
        routing_key = event["routing_key"] || event["handler_class"]
        return false unless routing_key

        @client.publish_to_topic(routing_key, event["payload"] || "{}")
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error replaying event: #{e.message}" }
        false
      end

      # Recurring tasks
      def recurring_tasks
        records = RecurringTask.order(:key).to_a
        last_runs = RecurringExecution
                    .where(task_key: records.map(&:key))
                    .select("task_key, MAX(run_at) AS run_at")
                    .group(:task_key)
                    .index_by(&:task_key)

        records.map do |record|
          last_exec = last_runs[record.key]
          task = Recurring::Task.from_configuration(record.key,
                                                    class: record.class_name,
                                                    command: record.command,
                                                    schedule: record.schedule,
                                                    queue: record.queue_name,
                                                    args: parse_arguments(record.arguments),
                                                    priority: record.priority,
                                                    description: record.description)

          {
            id: record.id,
            key: record.key,
            class_name: record.class_name,
            command: record.command,
            schedule: record.schedule,
            human_schedule: task.human_schedule,
            queue_name: record.queue_name,
            priority: record.priority,
            description: record.description,
            enabled: record.enabled,
            static: record.static,
            next_run_at: task.next_time,
            last_run_at: last_exec&.run_at,
            created_at: record.created_at,
            updated_at: record.updated_at
          }
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching recurring tasks: #{e.class}: #{e.message}" }
        []
      end

      def recurring_task(id)
        record = RecurringTask.find_by(id: id)
        return nil unless record

        task = Recurring::Task.from_configuration(record.key,
                                                  class: record.class_name,
                                                  command: record.command,
                                                  schedule: record.schedule,
                                                  queue: record.queue_name,
                                                  args: parse_arguments(record.arguments),
                                                  priority: record.priority,
                                                  description: record.description)

        executions = RecurringExecution.for_task(record.key).recent(25).map do |exec|
          { run_at: exec.run_at, created_at: exec.created_at }
        end

        {
          id: record.id,
          key: record.key,
          class_name: record.class_name,
          command: record.command,
          schedule: record.schedule,
          human_schedule: task.human_schedule,
          queue_name: record.queue_name,
          arguments: parse_arguments(record.arguments),
          priority: record.priority,
          description: record.description,
          enabled: record.enabled,
          static: record.static,
          next_run_at: task.next_time,
          executions: executions,
          created_at: record.created_at,
          updated_at: record.updated_at
        }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching recurring task #{id}: #{e.class}: #{e.message}" }
        nil
      end

      def toggle_recurring_task(id)
        record = RecurringTask.find_by(id: id)
        return nil unless record

        record.update!(enabled: !record.enabled)
        record.enabled ? :enabled : :disabled
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error toggling recurring task #{id}: #{e.message}" }
        nil
      end

      def enqueue_recurring_task_now(id)
        record = RecurringTask.find_by(id: id)
        return false unless record

        task = Recurring::Task.from_configuration(record.key,
                                                  class: record.class_name,
                                                  command: record.command,
                                                  schedule: record.schedule,
                                                  queue: record.queue_name,
                                                  args: parse_arguments(record.arguments),
                                                  priority: record.priority)

        schedule = Recurring::Schedule.new(config: Pgbus.configuration)
        schedule.enqueue_task(task, run_at: Time.now.utc)
        true
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error enqueuing recurring task #{id}: #{e.message}" }
        false
      end

      def recurring_tasks_count
        RecurringTask.count
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting recurring tasks: #{e.message}" }
        0
      end

      # Outbox
      def outbox_stats
        {
          unpublished: OutboxEntry.unpublished.count,
          total: OutboxEntry.count,
          oldest_unpublished_age: oldest_unpublished_age
        }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching outbox stats: #{e.message}" }
        { unpublished: 0, total: 0, oldest_unpublished_age: nil }
      end

      def outbox_entries(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        OutboxEntry.order(id: :desc).limit(per_page).offset(offset).to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching outbox entries: #{e.message}" }
        []
      end

      # Lock management
      def discard_lock(lock_key)
        UniquenessKey.where(lock_key: lock_key).delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding lock #{lock_key}: #{e.message}" }
        0
      end

      def discard_locks(lock_keys)
        return 0 if lock_keys.empty?

        UniquenessKey.where(lock_key: lock_keys).delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding locks: #{e.message}" }
        0
      end

      def discard_all_locks
        UniquenessKey.delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding all locks: #{e.message}" }
        0
      end

      # Job uniqueness keys
      def job_locks
        UniquenessKey.order(created_at: :desc).limit(100).map do |key|
          {
            lock_key: key.lock_key,
            queue_name: key.queue_name,
            msg_id: key.msg_id,
            created_at: key.created_at,
            age_seconds: key.created_at ? (Time.current - key.created_at).to_i : nil
          }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching uniqueness keys: #{e.message}" }
        []
      end

      # Job stats
      def job_stats_summary(minutes: 60)
        JobStat.summary(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching job stats summary: #{e.message}" }
        { total: 0, success: 0, failed: 0, dead_lettered: 0, avg_duration_ms: 0, max_duration_ms: 0 }
      end

      def job_throughput(minutes: 60)
        JobStat.throughput(minutes: minutes).map { |time, count| { time: time, count: count } }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching throughput: #{e.message}" }
        []
      end

      def job_status_counts(minutes: 60)
        JobStat.status_counts(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching status counts: #{e.message}" }
        {}
      end

      def slowest_job_classes(limit: 10, minutes: 60)
        JobStat.slowest_classes(limit: limit, minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching slowest classes: #{e.message}" }
        []
      end

      def latency_trend(minutes: 60)
        JobStat.latency_trend(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching latency trend: #{e.message}" }
        []
      end

      def latency_by_queue(minutes: 60)
        JobStat.avg_latency_by_queue(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching latency by queue: #{e.message}" }
        []
      end

      # Subscriber registry
      def registered_subscribers
        EventBus::Registry.instance.subscribers.map do |s|
          { pattern: s.pattern, handler_class: s.handler_class.name, queue_name: s.queue_name }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching subscribers: #{e.message}" }
        []
      end

      private

      def connection
        Pgbus::BusRecord.connection
      end

      # name is the full PGMQ queue name (already prefixed)
      def query_queue_messages(name, limit, offset)
        query_queue_messages_raw(name, limit, offset).map { |m| m.merge(queue: name) }
      end

      def query_queue_messages_raw(full_name, limit, offset)
        rows = connection.select_all(
          "SELECT * FROM pgmq.q_#{sanitize_name(full_name)} ORDER BY msg_id DESC LIMIT $1 OFFSET $2",
          "Pgbus Queue Messages",
          [limit, offset]
        )
        rows.to_a.map { |r| format_message(r, full_name) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error querying messages from #{full_name}: #{e.message}" }
        []
      end

      def all_queue_messages(limit, offset)
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        queues = queues_with_metrics.reject { |q| q[:name].end_with?(dlq_suffix) }
        messages = queues.flat_map do |q|
          query_queue_messages_raw(q[:name], limit + offset, 0)
        end
        messages.sort_by { |m| -m[:msg_id].to_i }.slice(offset, limit) || []
      end

      def queue_metrics_via_sql(queue_name)
        qtable = "q_#{sanitize_name(queue_name)}"
        seq_name = "#{qtable}_msg_id_seq"

        row = connection.select_one(<<~SQL, "Pgbus Queue Metrics")
          WITH q_summary AS (
            SELECT
              count(*) AS queue_length,
              count(CASE WHEN vt <= NOW() THEN 1 END) AS queue_visible_length,
              EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int AS newest_msg_age_sec,
              EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int AS oldest_msg_age_sec
            FROM pgmq.#{qtable}
          ),
          all_metrics AS (
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS total_messages
            FROM pgmq.#{seq_name}
          )
          SELECT
            q_summary.queue_length,
            q_summary.queue_visible_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            all_metrics.total_messages
          FROM q_summary, all_metrics
        SQL

        return nil unless row

        {
          name: queue_name,
          queue_length: row["queue_length"].to_i,
          queue_visible_length: row["queue_visible_length"].to_i,
          oldest_msg_age_sec: row["oldest_msg_age_sec"]&.to_i,
          newest_msg_age_sec: row["newest_msg_age_sec"]&.to_i,
          total_messages: row["total_messages"].to_i
        }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching metrics for #{queue_name}: #{e.class}: #{e.message}" }
        nil
      end

      def format_message(row, queue_name)
        {
          msg_id: row["msg_id"].to_i,
          read_ct: row["read_ct"].to_i,
          enqueued_at: row["enqueued_at"],
          last_read_at: row["last_read_at"],
          vt: row["vt"],
          message: row["message"],
          headers: row["headers"],
          queue_name: queue_name
        }
      end

      def format_process(row)
        heartbeat = row["last_heartbeat_at"]
        heartbeat_time = heartbeat.is_a?(String) ? Time.parse(heartbeat) : heartbeat
        stale = heartbeat_time && (Time.now - heartbeat_time) > Process::Heartbeat::ALIVE_THRESHOLD

        {
          id: row["id"].to_i,
          kind: row["kind"],
          hostname: row["hostname"],
          pid: row["pid"].to_i,
          metadata: row["metadata"].is_a?(String) ? JSON.parse(row["metadata"]) : row["metadata"],
          last_heartbeat_at: heartbeat_time,
          healthy: !stale,
          created_at: row["created_at"]
        }
      end

      def sanitize_name(name)
        QueueNameValidator.sanitize!(name)
      end

      def compute_throughput(queues)
        current_totals = queues.sum { |q| q[:total_messages] }
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        if @last_throughput_snapshot && @last_throughput_at
          elapsed = now - @last_throughput_at
          delta = current_totals - @last_throughput_snapshot
          rate = elapsed.positive? ? (delta / elapsed).round(1) : 0.0
        end

        @last_throughput_snapshot = current_totals
        @last_throughput_at = now

        rate || 0.0
      rescue StandardError
        0.0
      end

      def logical_queue_name(name)
        name
          .delete_prefix("#{Pgbus.configuration.queue_prefix}_")
          .sub(/_p\d+\z/, "")
      end

      def paused_queue_names
        QueueState.paused.pluck(:queue_name)
      rescue StandardError
        []
      end

      def oldest_unpublished_age
        oldest = OutboxEntry.unpublished.order(:id).pick(:created_at)
        return nil unless oldest

        (Time.now - oldest).to_i
      rescue StandardError
        nil
      end

      def parse_arguments(args)
        case args
        when Array then args
        when String then JSON.parse(args)
        when NilClass then []
        else Array(args)
        end
      rescue JSON::ParserError => e
        Pgbus.logger.debug { "[Pgbus::Web] Invalid recurring task arguments JSON: #{e.message}" }
        []
      end

      # --- Lock cleanup helpers ---

      # Extract uniqueness key from a queue message and release its lock.
      def release_lock_for_message(queue_name, msg_id)
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus Job Detail",
          [msg_id.to_i]
        )
        return unless row

        release_lock_for_payload(row["message"])
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing lock for message #{msg_id}: #{e.message}" }
      end

      # Extract uniqueness key from a JSON payload string and release its lock.
      def release_lock_for_payload(payload_str)
        return unless payload_str

        payload = payload_str.is_a?(String) ? JSON.parse(payload_str) : payload_str
        key = payload[Uniqueness::METADATA_KEY]
        UniquenessKey.release!(key) if key
      rescue JSON::ParserError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error parsing payload for lock release: #{e.message}" }
      end

      # Extract uniqueness keys from a collection of formatted messages and
      # release all associated locks in a single query.
      def release_locks_for_messages(messages)
        keys = messages.filter_map do |m|
          payload = m[:message]
          next unless payload

          parsed = payload.is_a?(String) ? JSON.parse(payload) : payload
          parsed[Uniqueness::METADATA_KEY]
        rescue JSON::ParserError
          nil
        end

        UniquenessKey.where(lock_key: keys).delete_all if keys.any?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing locks for messages: #{e.message}" }
      end

      # Collect uniqueness keys from all failed events and release their locks.
      def release_locks_for_failed_events
        rows = connection.select_all(
          "SELECT payload FROM pgbus_failed_events", "Pgbus Collect Failed Keys"
        )

        keys = rows.to_a.filter_map do |row|
          payload = JSON.parse(row["payload"])
          payload[Uniqueness::METADATA_KEY]
        rescue JSON::ParserError
          nil
        end

        UniquenessKey.where(lock_key: keys).delete_all if keys.any?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing locks for failed events: #{e.message}" }
      end

      # Archive the queue message a failed_event row points to. Idempotent —
      # silently no-ops if the message no longer exists in the queue.
      def archive_failed_message(event)
        return unless event["queue_name"] && event["msg_id"]

        @client.archive_message(event["queue_name"], event["msg_id"].to_i)
      rescue StandardError => e
        Pgbus.logger.debug do
          "[Pgbus::Web] Error archiving message for failed event #{event["id"]}: #{e.message}"
        end
      end

      # Archive every queue message referenced by a failed_event row.
      def archive_all_failed_messages
        rows = connection.select_all(
          "SELECT id, queue_name, msg_id FROM pgbus_failed_events WHERE msg_id IS NOT NULL",
          "Pgbus Collect Failed Messages"
        )
        rows.to_a.each { |row| archive_failed_message(row) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error archiving failed messages: #{e.message}" }
      end

      # Release all uniqueness keys associated with a queue before purge/drop.
      # Scans queue messages for uniqueness metadata and deletes matching rows.
      def release_uniqueness_keys_for_queue(queue_name)
        messages = query_queue_messages_raw(queue_name, 10_000, 0)
        release_locks_for_messages(messages)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing uniqueness keys for queue #{queue_name}: #{e.message}" }
      end
    end
  end
end
