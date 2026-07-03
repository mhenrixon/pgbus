# frozen_string_literal: true

module Pgbus
  module Metrics
    module Backends
      # Dependency-free, in-process Prometheus registry.
      #
      # Counters and gauges are stored per (name, sorted-tags) series. Histograms
      # are stored as a sum+count summary — cheap, dependency-free, and enough for
      # rate/avg dashboards without pre-declaring buckets. State is Mutex-guarded
      # (no unsynchronized shared state), since instrumentation fires from many
      # worker threads while the exporter renders from a Rack thread.
      #
      # Render output is Prometheus text exposition format (v0.0.4), served by
      # Pgbus::Metrics::PrometheusExporter.
      class Prometheus < Backend
        Series = Struct.new(:type, :samples) # samples: { tags_key => value }

        def initialize
          super
          @mutex = Mutex.new
          # name => Series
          @counters = {}
          @gauges = {}
          # name => { tags_key => { sum:, count:, tags: } }
          @histograms = {}
          # tags_key (sorted array) => original tags hash, for rendering labels
          @tag_labels = {}
        end

        def increment(name, value = 1, tags = {})
          key = tag_key(tags)
          @mutex.synchronize do
            series = (@counters[name] ||= {})
            series[key] = (series[key] || 0) + value
          end
          nil
        end

        def gauge(name, value, tags = {})
          key = tag_key(tags)
          @mutex.synchronize do
            series = (@gauges[name] ||= {})
            series[key] = value
          end
          nil
        end

        def histogram(name, value, tags = {})
          key = tag_key(tags)
          @mutex.synchronize do
            series = (@histograms[name] ||= {})
            bucket = (series[key] ||= { sum: 0, count: 0 })
            bucket[:sum] += value
            bucket[:count] += 1
          end
          nil
        end

        # Render the full registry as Prometheus text exposition format.
        def render
          @mutex.synchronize do
            lines = []
            @counters.each { |name, series| render_simple(lines, name, "counter", series) }
            @gauges.each   { |name, series| render_simple(lines, name, "gauge", series) }
            @histograms.each { |name, series| render_summary(lines, name, series) }
            "#{lines.join("\n")}\n"
          end
        end

        private

        def render_simple(lines, name, type, series)
          lines << "# TYPE #{name} #{type}"
          series.each do |key, value|
            lines << "#{name}#{labels_for(key)} #{format_number(value)}"
          end
        end

        def render_summary(lines, name, series)
          lines << "# TYPE #{name} summary"
          series.each do |key, bucket|
            labels = labels_for(key)
            lines << "#{name}_sum#{labels} #{format_number(bucket[:sum])}"
            lines << "#{name}_count#{labels} #{bucket[:count]}"
          end
        end

        # Stable, order-independent key for a tag set. Also memoizes the original
        # (stringified) tag hash so render can reproduce the label set.
        def tag_key(tags)
          normalized = tags.each_with_object({}) do |(k, v), acc|
            acc[k.to_s] = v.to_s
          end
          key = normalized.sort.freeze
          @tag_labels[key] ||= normalized
          key
        end

        def labels_for(key)
          labels = @tag_labels[key]
          return "" if labels.nil? || labels.empty?

          inner = key.map { |k, _| %(#{k}="#{escape_label(labels[k])}") }.join(",")
          "{#{inner}}"
        end

        # Prometheus label-value escaping: backslash, double-quote, newline.
        def escape_label(value)
          value.gsub("\\", "\\\\\\\\").gsub('"', '\\"').gsub("\n", '\\n')
        end

        def format_number(value)
          if value.is_a?(Float) && value.finite? && value == value.to_i
            value.to_i.to_s
          else
            value.to_s
          end
        end
      end
    end
  end
end
