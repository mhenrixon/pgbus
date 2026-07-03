# frozen_string_literal: true

module Pgbus
  module Metrics
    # Interface every metrics backend implements. The Subscriber only ever calls
    # these three methods, so a backend that satisfies them plugs in directly as
    # `config.metrics_backend`.
    #
    #   increment(name, value = 1, tags = {}) — monotonic counter add
    #   gauge(name, value, tags = {})         — point-in-time value
    #   histogram(name, value, tags = {})     — timing/size distribution sample
    #
    # `name` is a `pgbus_`-prefixed String; `tags` is a Hash of low-cardinality
    # label => value pairs (no msg_id, no event_id).
    class Backend
      def increment(_name, _value = 1, _tags = {})
        raise NotImplementedError, "#{self.class}#increment"
      end

      def gauge(_name, _value, _tags = {})
        raise NotImplementedError, "#{self.class}#gauge"
      end

      def histogram(_name, _value, _tags = {})
        raise NotImplementedError, "#{self.class}#histogram"
      end

      # No-op backend. Used as the resolved backend for a nil configuration so
      # callers never have to nil-check, though in practice the Subscriber is
      # simply not installed when metrics_backend is nil.
      class Null < Backend
        def increment(_name, _value = 1, _tags = {}) = nil
        def gauge(_name, _value, _tags = {}) = nil
        def histogram(_name, _value, _tags = {}) = nil
      end
    end
  end
end
