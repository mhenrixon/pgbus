# frozen_string_literal: true

module Pgbus
  # Pgbus::Streams is the SSE-based pub/sub subsystem that replaces
  # `turbo_stream_from`. The user-facing entrypoint is `Pgbus.stream(name)`,
  # which returns a `Pgbus::Streams::Stream` providing `#broadcast`,
  # `#current_msg_id`, and `#read_after`.
  module Streams
    # A handle on a single logical stream. The name can be any string, an
    # object responding to `to_gid_param`, or an array of streamables (which
    # are joined with colons — turbo-rails-compatible).
    #
    # Stream is *not* a singleton — callers create instances ad hoc, but the
    # underlying queue is created lazily and only once per process per name.
    class Stream
      attr_reader :name

      def initialize(streamables, client: Pgbus.client)
        @name = self.class.name_from(streamables)
        @client = client
        @ensured = false
        @ensure_mutex = Mutex.new
      end

      # Broadcasts a Turbo Stream HTML payload through the pgbus streamer.
      # PGMQ's `message` column is JSONB, so raw HTML strings can't be passed
      # directly. We wrap as `{"html": "..."}` on the way in and unwrap in
      # Pgbus::Web::Streamer::Dispatcher before delivering to the SSE client.
      # Callers pass a plain HTML string; the wrapping is an implementation
      # detail.
      def broadcast(payload)
        ensure_queue!
        @client.send_message(@name, { "html" => payload.to_s })
      end

      def current_msg_id
        @client.stream_current_msg_id(@name)
      end

      def read_after(after_id:, limit: 500)
        @client.read_after(@name, after_id: after_id, limit: limit)
      end

      def ensure!
        ensure_queue!
        self
      end

      # Mirrors `Turbo::Streams::StreamName#stream_name_from`. Strings pass
      # through; objects with `to_gid_param` or `to_param` are coerced; arrays
      # are joined with `:`. The result is suitable both as a logical stream
      # identifier and as the input to QueueNameValidator (after sanitisation).
      def self.name_from(streamables)
        if streamables.is_a?(Array)
          streamables.map { |s| name_from(s) }.join(":")
        elsif streamables.respond_to?(:to_gid_param)
          streamables.to_gid_param
        elsif streamables.respond_to?(:to_param) && !streamables.is_a?(Symbol)
          streamables.to_param
        else
          streamables.to_s
        end
      end

      private

      def ensure_queue!
        return if @ensured

        @ensure_mutex.synchronize do
          return if @ensured

          @client.ensure_stream_queue(@name)
          @ensured = true
        end
      end
    end
  end
end
