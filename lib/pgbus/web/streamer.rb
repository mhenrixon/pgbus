# frozen_string_literal: true

module Pgbus
  module Web
    # The worker-local coordinator that owns SSE connections, one PG LISTEN
    # session, and the dispatch/heartbeat threads. Lazily created on the
    # first SSE connection to a Puma worker (or eagerly in tests). There is
    # exactly one Instance per Puma worker process; the module-level
    # accessors memoise it.
    #
    # This is NOT a Singleton in the GoF sense — tests are free to construct
    # throwaway Instances directly and dependency-inject everything. The
    # `current` / `reset!` helpers exist purely so the Rack StreamApp can
    # share an instance across requests within a worker without passing it
    # through every method call.
    module Streamer
      class << self
        # Returns the worker-local instance, creating it on first call.
        # `factory_opts` are passed to `Instance.new` the first time.
        def current(**factory_opts)
          @current ||= Instance.new(**factory_opts).tap(&:start)
        end

        # Explicitly set the current instance — used by tests and by the
        # Puma plugin to inject a pre-built instance.
        attr_writer :current

        # Tear down the current instance and clear the slot. Called by the
        # Puma shutdown hook (Phase 4.4) and by tests between examples.
        def reset!
          instance = @current
          @current = nil
          instance&.shutdown!
        end
      end
    end
  end
end
