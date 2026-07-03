# frozen_string_literal: true

module Pgbus
  # Backend-agnostic metrics adapter.
  #
  # Pgbus emits ~16 `pgbus.*` ActiveSupport::Notifications events (see
  # Pgbus::Instrumentation). The AppSignal integration is one consumer of those
  # events; this namespace is a second, vendor-neutral consumer that forwards the
  # same event→metric mapping to a configurable backend so teams on Prometheus,
  # Datadog, or plain StatsD get metrics without hand-writing subscribers.
  #
  # Opt in via `config.metrics_backend = :prometheus | :statsd | <Backend>`. The
  # default is nil, which installs no subscription (zero overhead). Installation
  # happens from the engine, mirroring the AppSignal initializer.
  module Metrics
    module_function

    # Coerce a configuration value into a concrete Backend instance.
    #
    #   nil          -> Backend::Null (no-op)
    #   :prometheus  -> Backends::Prometheus
    #   :statsd      -> Backends::Statsd (host/port from configuration)
    #   Backend inst -> returned unchanged
    def resolve_backend(value)
      case value
      when nil
        Backend::Null.new
      when :prometheus
        Backends::Prometheus.new
      when :statsd
        config = Pgbus.configuration
        Backends::Statsd.new(host: config.statsd_host, port: config.statsd_port)
      when Backend
        value
      else
        raise ArgumentError,
              "metrics_backend must be nil, :prometheus, :statsd, or a " \
              "Pgbus::Metrics::Backend instance (got #{value.inspect})"
      end
    end
  end
end
