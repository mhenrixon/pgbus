# frozen_string_literal: true

module Pgbus
  module Streams
    # Rack middleware that clears the per-request thread-local watermark
    # cache used by `Pgbus::StreamsHelper#pgbus_stream_from`. Without this
    # middleware, a subsequent request served by the same thread would
    # see stale `current_msg_id` values from the previous render — the
    # pgbus_stream_from helper caches watermark lookups within a single
    # request to avoid N+1 queries when a page uses multiple streams,
    # and the cache would leak without a per-request boundary.
    #
    # Installed automatically by `Pgbus::Engine` at boot.
    class WatermarkCacheMiddleware
      CACHE_KEY = :pgbus_streams_watermark_cache

      def initialize(app)
        @app = app
      end

      def call(env)
        @app.call(env)
      ensure
        Thread.current[CACHE_KEY] = nil
      end
    end
  end
end
