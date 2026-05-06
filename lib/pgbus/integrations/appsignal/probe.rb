# frozen_string_literal: true

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
              tags = { queue: q[:name] }
              gauge "queue_depth", q[:queue_length], tags
              gauge "queue_visible_depth", q[:queue_visible_length], tags
              gauge "queue_paused", q[:paused] ? 1 : 0, tags
              age = q[:oldest_msg_age_sec]
              gauge "queue_oldest_message_age_seconds", age, tags if age
            end
          rescue StandardError => e
            log_failure("queue metrics", e)
          end

          def track_processes
            gauge "active_processes", data_source.processes.count
          rescue StandardError => e
            log_failure("process metrics", e)
          end

          def track_summary
            stats = data_source.summary_stats
            gauge "total_queues", stats[:total_queues]
            gauge "total_depth", stats[:total_depth]
            gauge "total_visible", stats[:total_visible]
            gauge "dlq_depth", stats[:dlq_depth]
            gauge "failed_events_total", stats[:failed_count]
            gauge "throughput_rate", stats[:throughput_rate]
            gauge "total_dead_tuples", stats[:total_dead_tuples]
            gauge "tables_needing_vacuum", stats[:tables_needing_vacuum]
            gauge "oldest_transaction_age_seconds", stats[:oldest_transaction_age_sec]
          rescue StandardError => e
            log_failure("summary metrics", e)
          end

          def track_streams
            return unless data_source.respond_to?(:stream_stats_available?) &&
                          data_source.stream_stats_available?

            summary = data_source.stream_stats_summary
            gauge "stream_broadcasts_60m", summary[:broadcasts]
            gauge "stream_connects_60m", summary[:connects]
            gauge "stream_disconnects_60m", summary[:disconnects]
            gauge "stream_active_connections", summary[:active_estimate]
            gauge "stream_avg_fanout", summary[:avg_fanout]
            gauge "stream_avg_broadcast_ms", summary[:avg_broadcast_ms]
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
