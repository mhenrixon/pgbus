# frozen_string_literal: true

require "time"

module Pgbus
  module Web
    class DataSource
      def initialize(client: Pgbus.client)
        @client = client
      end

      # Dashboard summary
      def summary_stats
        queues = queues_with_metrics
        total_depth = queues.sum { |q| q[:queue_length] }
        total_visible = queues.sum { |q| q[:queue_visible_length] }
        dlq_suffix = Pgbus.configuration.dead_letter_queue_suffix
        dlq_depth = queues.select { |q| q[:name].end_with?(dlq_suffix) }.sum { |q| q[:queue_length] }

        {
          total_queues: queues.size,
          total_depth: total_depth,
          total_visible: total_visible,
          active_processes: processes.count,
          failed_count: failed_events_count,
          dlq_depth: dlq_depth
        }
      end

      # Queues — query via ActiveRecord for reliability in web processes
      # (avoids PGMQ client connection issues when the web server uses a
      # different connection lifecycle than the worker processes).
      def queues_with_metrics
        queue_names = connection.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        queue_names.map { |name| queue_metrics_via_sql(name) }.compact
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
        @client.purge_queue(name)
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
        @client.set_visibility_timeout(queue_name, msg_id.to_i, vt: 0)
      end

      def discard_job(queue_name, msg_id)
        @client.archive_message(queue_name, msg_id.to_i)
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

        payload = JSON.parse(event["payload"])
        headers = event["headers"]
        headers = JSON.parse(headers) if headers.is_a?(String)

        connection.transaction do
          @client.send_message(event["queue_name"], payload, headers: headers)
          connection.execute("DELETE FROM pgbus_failed_events WHERE id = #{id.to_i}")
        end
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error retrying failed event #{id}: #{e.message}" }
        false
      end

      def discard_failed_event(id)
        connection.execute("DELETE FROM pgbus_failed_events WHERE id = #{id.to_i}")
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding failed event #{id}: #{e.message}" }
        false
      end

      def retry_all_failed
        count = 0
        connection.select_all("SELECT * FROM pgbus_failed_events").each do |event|
          payload = JSON.parse(event["payload"])
          headers = event["headers"]
          headers = JSON.parse(headers) if headers.is_a?(String)

          connection.transaction do
            @client.send_message(event["queue_name"], payload, headers: headers)
            connection.execute("DELETE FROM pgbus_failed_events WHERE id = #{event["id"].to_i}")
          end
          count += 1
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Failed to retry event #{event["id"]}: #{e.message}" }
        end
        count
      end

      def discard_all_failed
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
        @client.delete_from_queue(queue_name, msg_id.to_i)
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
        count = 0
        messages.each do |m|
          discard_dlq_message(m[:queue_name], m[:msg_id]) && count += 1
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error discarding DLQ message #{m[:msg_id]}: #{e.message}" }
          next
        end
        count
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
        ActiveRecord::Base.connection
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
        name.gsub(/[^a-zA-Z0-9_]/, "")
      end
    end
  end
end
