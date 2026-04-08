# frozen_string_literal: true

module Pgbus
  module Streams
    # Process-wide registry of server-side audience filter predicates.
    # Used by `Pgbus.stream(name).broadcast(html, visible_to: :label)`
    # to restrict delivery to connections whose authorize-hook context
    # matches the predicate.
    #
    # Typical setup at boot time:
    #
    #   Pgbus::Streams.filters.register(:admin_only) { |user| user.admin? }
    #   Pgbus::Streams.filters.register(:workspace_member) do |user, stream|
    #     user.workspaces.pluck(:id).include?(stream.split(":").last.to_i)
    #   end
    #
    # Broadcasts reference filters by label:
    #
    #   Pgbus.stream("workspace:42").broadcast(html, visible_to: :admin_only)
    #
    # The Dispatcher looks up the filter in the registry, evaluates it
    # against each connection's context (populated from the StreamApp's
    # authorize hook return value), and only delivers to connections
    # where the predicate returns true.
    #
    # Why a registry of labels instead of passing a Proc directly to
    # broadcast: predicates can't be serialized to JSON, so they can't
    # travel through PGMQ. The label is serialized; the predicate lives
    # in-process on the subscriber side. This also means the predicate
    # is evaluated on the same process that holds the SSE connection,
    # so the user context (typically an ActiveRecord model or a session
    # hash) is always available.
    class Filters
      def initialize(logger: nil)
        @mutex = Mutex.new
        @filters = {}
        @logger = logger
      end

      def register(label, callable = nil, &block)
        raise ArgumentError, "filter label must be a Symbol (got #{label.class})" unless label.is_a?(Symbol)

        predicate = callable || block
        raise ArgumentError, "filter must be given a block or callable" if predicate.nil?

        @mutex.synchronize { @filters[label] = predicate }
      end

      def lookup(label)
        @mutex.synchronize { @filters[label] }
      end

      # Evaluates the named filter against a context. The context is
      # whatever the StreamApp's authorize hook returned when the
      # connection was established — typically a user model.
      #
      # Policy decisions:
      #   - label=nil → visible (no filter attached to the broadcast)
      #   - unknown label → NOT visible + warning log. Fail-closed so
      #     a typo or renamed filter doesn't turn a restricted
      #     broadcast into a public one. The whole point of audience
      #     filtering is data isolation; failing open on a typo
      #     defeats the feature. The warning log is loud enough that
      #     typos still get noticed in dev (check the log or wonder
      #     why no subscriber sees your broadcast).
      #   - predicate raises → NOT visible (fail-closed on runtime
      #     error to avoid leaking data on an exception path).
      def visible?(label, context)
        return true if label.nil?

        predicate = lookup(label)
        if predicate.nil?
          log_warn("unknown filter label #{label.inspect} — broadcast dropped (fail-closed)")
          return false
        end

        begin
          !!predicate.call(context)
        rescue StandardError => e
          log_error("filter #{label.inspect} raised #{e.class}: #{e.message} — dropping broadcast (fail-closed)")
          false
        end
      end

      private

      def log_warn(message)
        logger = @logger || (Pgbus.logger if defined?(Pgbus) && Pgbus.respond_to?(:logger))
        logger&.warn { "[Pgbus::Streams::Filters] #{message}" }
      end

      def log_error(message)
        logger = @logger || (Pgbus.logger if defined?(Pgbus) && Pgbus.respond_to?(:logger))
        logger&.error { "[Pgbus::Streams::Filters] #{message}" }
      end
    end
  end
end
