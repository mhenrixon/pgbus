# frozen_string_literal: true

require "socket"

module Pgbus
  module Integrations
    module Appsignal
      # Minutely probe that pushes pgbus-wide gauges into AppSignal.
      #
      # All readings come from Pgbus::Web::DataSource so the probe doesn't
      # duplicate query logic. DataSource is built to be resilient — every
      # method rescues StandardError and returns a safe default — but we
      # still wrap each section in our own rescue so a probe iteration
      # never raises out into the AppSignal probe runner.
      #
      # Every gauge includes a `hostname` tag so AppSignal magic dashboards
      # can filter per host (matching the Sidekiq/Puma probe convention).
      module Probe
        METRIC_PREFIX = "pgbus_"
        private_constant :METRIC_PREFIX

        class << self
          def install! # rubocop:disable Naming/PredicateMethod
            return false if @installed

            ::Appsignal::Probes.register :pgbus, new_probe_instance
            @installed = true
            true
          end

          def installed?
            @installed == true
          end

          def reset!
            ::Appsignal::Probes.unregister(:pgbus) if defined?(::Appsignal::Probes) &&
                                                      ::Appsignal::Probes.respond_to?(:unregister)
            @installed = false
          end

          # Visible for testing — returns a fresh runnable probe.
          def new_probe_instance
            Runner.new
          end
        end

        # The actual probe object; AppSignal calls #call once per minute.
        class Runner
          def initialize(data_source: nil)
            @data_source = data_source
            @hostname = Socket.gethostname
          end

          def call
            return unless data_source

            track_queues
            track_processes
            track_summary
            track_streams
          end

          private

          def data_source
            @data_source ||=
              (::Pgbus::Web::DataSource.new if defined?(::Pgbus::Web::DataSource))
          end

          def track_queues
            data_source.queues_with_metrics.each do |q|
              tags = { queue: q[:name], hostname: @hostname }
              gauge "queue_depth", q[:queue_length], tags
              gauge "queue_visible_depth", q[:queue_visible_length], tags
              gauge "queue_paused", q[:paused] ? 1 : 0, tags
              age = q[:oldest_msg_age_sec]
              if age
                gauge "queue_oldest_message_age_seconds", age, tags
                gauge "queue_latency", age * 1_000, tags
              end
            end
          rescue StandardError => e
            log_failure("queue metrics", e)
          end

          def track_processes
            gauge "active_processes", data_source.processes.count, { hostname: @hostname }
          rescue StandardError => e
            log_failure("process metrics", e)
          end

          def track_summary
            stats = data_source.summary_stats
            host = { hostname: @hostname }
            gauge "total_queues", stats[:total_queues], host
            gauge "total_depth", stats[:total_depth], host
            gauge "total_visible", stats[:total_visible], host
            gauge "dlq_depth", stats[:dlq_depth], host
            gauge "failed_events_total", stats[:failed_count], host
            gauge "throughput_rate", stats[:throughput_rate], host
            gauge "total_dead_tuples", stats[:total_dead_tuples], host
            gauge "tables_needing_vacuum", stats[:tables_needing_vacuum], host
            gauge "oldest_transaction_age_seconds", stats[:oldest_transaction_age_sec], host
          rescue StandardError => e
            log_failure("summary metrics", e)
          end

          def track_streams
            return unless data_source.respond_to?(:stream_stats_available?) &&
                          data_source.stream_stats_available?

            summary = data_source.stream_stats_summary
            host = { hostname: @hostname }
            gauge "stream_broadcasts_60m", summary[:broadcasts], host
            gauge "stream_connects_60m", summary[:connects], host
            gauge "stream_disconnects_60m", summary[:disconnects], host
            gauge "stream_active_connections", summary[:active_estimate], host
            gauge "stream_avg_fanout", summary[:avg_fanout], host
            gauge "stream_avg_broadcast_ms", summary[:avg_broadcast_ms], host
          rescue StandardError => e
            log_failure("stream metrics", e)
          end

          def gauge(key, value, tags = {})
            return if value.nil?

            ::Appsignal.set_gauge("#{METRIC_PREFIX}#{key}", value, tags)
          end

          def log_failure(label, error)
            Pgbus.logger.debug do
              "[Pgbus::AppSignal::Probe] #{label} failed: #{error.class}: #{error.message}"
            end
          end
        end
      end
    end
  end
end
