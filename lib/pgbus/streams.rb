# frozen_string_literal: true

module Pgbus
  # Pgbus::Streams is the SSE-based pub/sub subsystem that replaces
  # `turbo_stream_from`. The user-facing entrypoint is `Pgbus.stream(name)`,
  # which returns a `Pgbus::Streams::Stream` providing `#broadcast`,
  # `#current_msg_id`, and `#read_after`.
  module Streams
    # Raised when a composed stream name would overflow PGMQ's queue-name
    # budget (derived from PostgreSQL's NAMEDATALEN=64, minus PGMQ's `q_`
    # table prefix, minus the configured queue_prefix + separator).
    #
    # Inherits from ArgumentError so existing rescues of the underlying
    # QueueNameValidator error keep working; callers that want to handle
    # this specifically can rescue Pgbus::Streams::StreamNameTooLong.
    class StreamNameTooLong < ArgumentError; end

    # Process-wide registry of server-side audience filter predicates.
    # Register filters at boot time via:
    #   Pgbus::Streams.filters.register(:admin_only) { |user| user.admin? }
    # See lib/pgbus/streams/filters.rb for the full API.
    def self.filters
      @filters ||= Filters.new
    end

    # Clears the filters registry. Used by tests; not intended for runtime.
    def self.reset_filters!
      @filters = nil
    end

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
        self.class.validate_name_length!(@name, streamables)
        @client = client
        @ensured = false
        @ensure_mutex = Mutex.new
      end

      # Broadcasts a Turbo Stream HTML payload through the pgbus streamer.
      # PGMQ's `message` column is JSONB, so raw HTML strings can't be passed
      # directly. We wrap as `{"html": "..."}` on the way in and unwrap in
      # Pgbus::Web::Streamer::StreamEventDispatcher before delivering to the SSE client.
      # Callers pass a plain HTML string; the wrapping is an implementation
      # detail.
      #
      # Transactional semantics: if this call is made inside an open
      # ActiveRecord transaction, the PGMQ insert is deferred to an
      # after_commit callback. If the transaction rolls back, the broadcast
      # silently drops — clients never see the change that the database
      # never persisted. This is the feature no other Rails real-time stack
      # (including turbo-rails over ActionCable) can offer: the broadcast
      # and the data mutation are atomic with respect to each other.
      # Returns the assigned msg_id when sent synchronously, nil when
      # deferred (the id isn't known until the after_commit callback runs).
      #
      # Audience filtering: pass `visible_to:` with a filter label (a
      # Symbol previously registered via Pgbus::Streams.filters.register)
      # to restrict delivery to connections whose authorize-hook context
      # satisfies the predicate. The label travels with the broadcast
      # through PGMQ; the predicate itself lives in-process on the
      # subscriber side and is evaluated per-connection by the Dispatcher.
      def broadcast(payload, visible_to: nil)
        ensure_queue!
        wrapped = { "html" => payload.to_s }
        wrapped["visible_to"] = visible_to.to_s if visible_to
        transaction = current_open_transaction
        if transaction
          transaction.after_commit { @client.send_message(@name, wrapped) }
          nil
        else
          @client.send_message(@name, wrapped)
        end
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

      # Returns a Pgbus::Streams::Presence handle for this stream.
      # The Presence object exposes join/leave/touch/members/sweep!
      # for tracking who is currently subscribed. See
      # lib/pgbus/streams/presence.rb for the API.
      def presence
        @presence ||= Presence.new(self)
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

      # Enforces the pgbus queue-name budget at the Stream-construction
      # boundary so a forgotten call site fails with an actionable error
      # (pointing at the offending streamables and suggesting
      # `Pgbus.stream_key`) instead of an opaque QueueNameValidator
      # failure three frames deep in Client#ensure_stream_queue.
      #
      # The budget is computed from `config.queue_prefix` at call time
      # so apps that override the prefix get the correct limit. Does not
      # mutate the name — silent truncation is a footgun for
      # multi-tenant apps where collisions would mix broadcasts across
      # records. Callers who need a short, safe identifier should use
      # `Pgbus.stream_key(...)` or include `Pgbus::Streams::Streamable`
      # on their ActiveRecord models.
      def self.validate_name_length!(name, streamables)
        budget = Key.queue_name_budget
        return if name.length <= budget

        raise StreamNameTooLong,
              "Stream name #{name.inspect} is #{name.length} chars, " \
              "exceeds pgbus budget of #{budget} " \
              "(queue_prefix=#{Pgbus.configuration.queue_prefix.inspect}, " \
              "NAMEDATALEN=#{QueueNameValidator::MAX_QUEUE_NAME_LENGTH}). " \
              "Streamables: #{streamables.inspect}. " \
              "Use Pgbus.stream_key(*streamables) to produce a safe short name, " \
              "or include Pgbus::Streams::Streamable on the model."
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

      # Returns the current AR transaction if one is open, nil otherwise.
      # Guarded by defined? so the streams subsystem stays usable in
      # non-Rails contexts. AR's NullTransaction yields immediately from
      # after_commit, so we have to check #open? explicitly — otherwise
      # every call path would hit the "deferred" branch and we'd lose the
      # msg_id return value.
      def current_open_transaction
        return nil unless defined?(::ActiveRecord::Base)

        connection = ::ActiveRecord::Base.connection
        transaction = connection.current_transaction
        transaction if transaction.open?
      rescue StandardError => e
        # Defensive: if AR is loaded but not yet connected (e.g. a
        # Rake task invoked before Rails boot), don't let the transaction
        # probe break the broadcast. Debug-logged so a misconfigured
        # app can diagnose "why aren't my transactional broadcasts
        # deferring?" without impacting production performance
        # (debug is off by default).
        Pgbus.logger.debug do
          "[Pgbus::Streams::Stream] transaction probe failed (#{e.class}: #{e.message}); " \
            "falling back to synchronous broadcast"
        end
        nil
      end
    end
  end
end
