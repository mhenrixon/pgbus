# frozen_string_literal: true

module Pgbus
  module Web
    # Converts DataSource output into Prometheus text exposition format
    # (Content-Type: text/plain; version=0.0.4; charset=utf-8).
    #
    # Each metric family gets a HELP line, a TYPE line, and one or more
    # sample lines. Labels are double-quoted per the Prometheus spec.
    # All timing values are converted from milliseconds to seconds.
    #
    # Resilient by design: each section rescues StandardError independently
    # so a failure in one data source method doesn't blank the entire
    # scrape response.
    class MetricsSerializer
      def initialize(data_source)
        @data_source = data_source
      end

      def serialize
        lines = []
        append_queue_metrics(lines)
        append_job_metrics(lines)
        append_process_metrics(lines)
        append_summary_metrics(lines)
        append_stream_metrics(lines)
        "#{lines.join("\n")}\n"
      end

      private

      def append_queue_metrics(lines)
        queues = @data_source.queues_with_metrics
        return if queues.empty?

        gauge(lines, "pgbus_queue_depth", "Number of messages in the queue (including invisible)") do
          queues.map { |q| [q[:queue_length], { queue: q[:name] }] }
        end

        gauge(lines, "pgbus_queue_visible_depth", "Number of visible (ready to read) messages") do
          queues.map { |q| [q[:queue_visible_length], { queue: q[:name] }] }
        end

        gauge(lines, "pgbus_queue_total_messages", "Total messages ever enqueued") do
          queues.map { |q| [q[:total_messages], { queue: q[:name] }] }
        end

        gauge(lines, "pgbus_queue_oldest_message_age_seconds", "Age of the oldest message in seconds") do
          queues.filter_map do |q|
            next unless q[:oldest_msg_age_sec]

            [q[:oldest_msg_age_sec], { queue: q[:name] }]
          end
        end

        gauge(lines, "pgbus_queue_paused", "Whether the queue is paused (1) or active (0)") do
          queues.map { |q| [q[:paused] ? 1 : 0, { queue: q[:name] }] }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Metrics] Error serializing queue metrics: #{e.message}" }
      end

      def append_job_metrics(lines)
        counts = @data_source.job_status_counts
        unless counts.empty?
          gauge(lines, "pgbus_jobs_total", "Number of jobs by status in the stats window") do
            counts.map { |status, count| [count, { status: status }] }
          end
        end

        summary = @data_source.job_stats_summary
        if summary[:total].positive?
          gauge(lines, "pgbus_job_duration_avg_seconds", "Average job duration in seconds") do
            [[ms_to_s(summary[:avg_duration_ms])]]
          end

          gauge(lines, "pgbus_job_duration_max_seconds", "Maximum job duration in seconds") do
            [[ms_to_s(summary[:max_duration_ms])]]
          end
        end

        return unless Pgbus::JobStat.latency_columns? && summary[:avg_latency_ms]

        gauge(lines, "pgbus_job_enqueue_latency_seconds", "Enqueue latency percentiles in seconds") do
          [
            [ms_to_s(summary[:p50_latency_ms]), { quantile: "0.5" }],
            [ms_to_s(summary[:p95_latency_ms]), { quantile: "0.95" }],
            [ms_to_s(summary[:p99_latency_ms]), { quantile: "0.99" }]
          ]
        end

        gauge(lines, "pgbus_job_avg_retries", "Average retry count per job") do
          [[summary[:avg_retries]]]
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Metrics] Error serializing job metrics: #{e.message}" }
      end

      def append_process_metrics(lines)
        count = @data_source.processes.count
        gauge(lines, "pgbus_active_processes", "Number of active pgbus worker processes") do
          [[count]]
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Metrics] Error serializing process metrics: #{e.message}" }
      end

      def append_summary_metrics(lines)
        stats = @data_source.summary_stats
        gauge(lines, "pgbus_failed_events_total", "Total failed events") do
          [[stats[:failed_count]]]
        end

        gauge(lines, "pgbus_dlq_depth", "Total messages across all dead letter queues") do
          [[stats[:dlq_depth]]]
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Metrics] Error serializing summary metrics: #{e.message}" }
      end

      def append_stream_metrics(lines)
        return unless @data_source.stream_stats_available?

        summary = @data_source.stream_stats_summary
        gauge(lines, "pgbus_stream_events_total", "Stream events by type in the stats window") do
          [
            [summary[:broadcasts], { event_type: "broadcast" }],
            [summary[:connects], { event_type: "connect" }],
            [summary[:disconnects], { event_type: "disconnect" }]
          ]
        end

        gauge(lines, "pgbus_stream_active_connections", "Estimated active SSE connections") do
          [[summary[:active_estimate]]]
        end

        gauge(lines, "pgbus_stream_avg_fanout", "Average broadcast fanout (subscribers per broadcast)") do
          [[summary[:avg_fanout]]]
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Metrics] Error serializing stream metrics: #{e.message}" }
      end

      # Emits a Prometheus gauge metric family. The block must return an array
      # of [value] or [value, { label: "val" }] pairs.
      def gauge(lines, name, help)
        samples = yield
        return if samples.empty?

        lines << "# HELP #{name} #{help}"
        lines << "# TYPE #{name} gauge"
        samples.each do |value, labels|
          lines << format_sample(name, value, labels)
        end
      end

      def format_sample(name, value, labels = nil)
        if labels && !labels.empty?
          label_str = labels.map { |k, v| "#{k}=\"#{v}\"" }.join(",")
          "#{name}{#{label_str}} #{format_value(value)}"
        else
          "#{name} #{format_value(value)}"
        end
      end

      def format_value(value)
        value.is_a?(Float) ? value.to_s : value.to_i.to_s
      end

      def ms_to_s(milliseconds)
        (milliseconds.to_f / 1000).round(4)
      end
    end
  end
end
