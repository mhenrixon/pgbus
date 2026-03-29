# frozen_string_literal: true

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
        dlq_depth = queues.select { |q| q[:name].end_with?("_dlq") }.sum { |q| q[:queue_length] }

        {
          total_queues: queues.size,
          total_depth: total_depth,
          total_visible: total_visible,
          active_processes: processes.count,
          failed_count: failed_events_count,
          dlq_depth: dlq_depth
        }
      end

      # Queues
      def queues_with_metrics
        metrics = @client.metrics || []
        Array(metrics).map { |m| format_metrics(m) }
      rescue StandardError
        []
      end

      def queue_detail(name)
        m = @client.metrics(name)
        m ? format_metrics(m) : nil
      rescue StandardError
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
        full_name = Pgbus.configuration.queue_name(queue_name)
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(full_name)} WHERE msg_id = $1",
          "Pgbus Job Detail",
          [msg_id.to_i]
        )
        row ? format_message(row, queue_name) : nil
      rescue StandardError
        nil
      end

      def retry_job(queue_name, msg_id)
        full_name = Pgbus.configuration.queue_name(queue_name)
        # Reset visibility so it gets picked up again
        @client.pgmq.set_vt(full_name, msg_id.to_i, vt: 0)
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
      rescue StandardError
        []
      end

      def failed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_failed_events")
        result.to_i
      rescue StandardError
        0
      end

      def failed_event(id)
        connection.select_one(
          "SELECT * FROM pgbus_failed_events WHERE id = $1",
          "Pgbus Failed Event",
          [id.to_i]
        )
      rescue StandardError
        nil
      end

      def retry_failed_event(id)
        event = failed_event(id)
        return false unless event

        @client.send_message(event["queue_name"], JSON.parse(event["payload"]))
        connection.execute("DELETE FROM pgbus_failed_events WHERE id = #{id.to_i}")
        true
      rescue StandardError
        false
      end

      def discard_failed_event(id)
        connection.execute("DELETE FROM pgbus_failed_events WHERE id = #{id.to_i}")
        true
      rescue StandardError
        false
      end

      def retry_all_failed
        events = connection.select_all("SELECT * FROM pgbus_failed_events")
        events.each do |event|
          @client.send_message(event["queue_name"], JSON.parse(event["payload"]))
        end
        connection.execute("TRUNCATE pgbus_failed_events")
        events.count
      rescue StandardError
        0
      end

      def discard_all_failed
        result = connection.execute("DELETE FROM pgbus_failed_events")
        result.cmd_tuples
      rescue StandardError
        0
      end

      # Dead letter queue
      def dlq_messages(page: 1, per_page: 25)
        queues = queues_with_metrics.select { |q| q[:name].end_with?("_dlq") }
        offset = (page - 1) * per_page

        messages = queues.flat_map do |q|
          query_queue_messages_raw(q[:name], 100, 0)
        end

        messages.sort_by { |m| -m[:msg_id].to_i }.slice(offset, per_page) || []
      rescue StandardError
        []
      end

      def retry_dlq_message(queue_name, msg_id)
        full_dlq = Pgbus.configuration.queue_name(queue_name)
        original_queue = full_dlq.delete_suffix(Pgbus.configuration.dead_letter_queue_suffix)

        # Read the message from DLQ
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(full_dlq)} WHERE msg_id = $1",
          "Pgbus DLQ Read",
          [msg_id.to_i]
        )
        return false unless row

        # Send back to original queue and delete from DLQ
        @client.pgmq.produce(original_queue, row["message"])
        @client.pgmq.delete(full_dlq, msg_id.to_i)
        true
      rescue StandardError
        false
      end

      def discard_dlq_message(queue_name, msg_id)
        full_name = Pgbus.configuration.queue_name(queue_name)
        @client.pgmq.delete(full_name, msg_id.to_i)
        true
      rescue StandardError
        false
      end

      # Processes
      def processes
        rows = connection.select_all(
          "SELECT * FROM pgbus_processes ORDER BY kind, created_at"
        )
        rows.to_a.map { |r| format_process(r) }
      rescue StandardError
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
      rescue StandardError
        []
      end

      def processed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_processed_events")
        result.to_i
      rescue StandardError
        0
      end

      # Subscriber registry
      def registered_subscribers
        EventBus::Registry.instance.subscribers.map do |s|
          { pattern: s.pattern, handler_class: s.handler_class.name, queue_name: s.queue_name }
        end
      rescue StandardError
        []
      end

      private

      def connection
        ActiveRecord::Base.connection
      end

      def query_queue_messages(name, limit, offset)
        full_name = Pgbus.configuration.queue_name(name)
        query_queue_messages_raw(full_name, limit, offset).map { |m| m.merge(queue: name) }
      end

      def query_queue_messages_raw(full_name, limit, offset)
        rows = connection.select_all(
          "SELECT * FROM pgmq.q_#{sanitize_name(full_name)} ORDER BY msg_id DESC LIMIT $1 OFFSET $2",
          "Pgbus Queue Messages",
          [limit, offset]
        )
        rows.to_a.map { |r| format_message(r, full_name) }
      rescue StandardError
        []
      end

      def all_queue_messages(limit, offset)
        queues = queues_with_metrics.reject { |q| q[:name].end_with?("_dlq") }
        messages = queues.flat_map do |q|
          query_queue_messages_raw(q[:name], 100, 0)
        end
        messages.sort_by { |m| -m[:msg_id].to_i }.slice(offset, limit) || []
      end

      def format_metrics(m)
        {
          name: m.queue_name.to_s,
          queue_length: m.queue_length.to_i,
          queue_visible_length: m.queue_visible_length.to_i,
          oldest_msg_age_sec: m.oldest_msg_age_sec&.to_i,
          newest_msg_age_sec: m.newest_msg_age_sec&.to_i,
          total_messages: m.total_messages.to_i
        }
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
