# frozen_string_literal: true

module Pgbus
  module Streams
    # Runtime patch that redirects `turbo_stream_from` (the view helper)
    # through `pgbus_stream_from` when pgbus streams are enabled. This is
    # the subscribe-side counterpart to `TurboBroadcastable` (which patches
    # the publish-side `broadcast_stream_to`).
    #
    # Without this patch, third-party gems like hotwire-livereload call
    # `turbo_stream_from "hotwire-livereload"` in their views, which
    # renders a `<turbo-cable-stream-source>` element connected to
    # ActionCable. Meanwhile, the `TurboBroadcastable` patch routes the
    # broadcast through PGMQ/SSE. Publisher and subscriber end up on
    # different transports — the message never arrives.
    #
    # After this patch, `turbo_stream_from` renders a `<pgbus-stream-source>`
    # element instead, so both sides use PGMQ/SSE. When `streams_enabled`
    # is false, the original turbo-rails behavior is preserved via `super`.
    #
    # The `include Pgbus::StreamsHelper` is required because some callers
    # invoke `turbo_stream_from` from a Rack middleware context (e.g.
    # hotwire-livereload's Middleware uses `ActionController::Base.helpers`)
    # where `Turbo::StreamsHelper` is available but `Pgbus::StreamsHelper`
    # is not — the engine's `isolate_namespace` scopes helpers to its own
    # views. Including it here ensures `pgbus_stream_from` is always
    # reachable on the receiver.
    module TurboStreamOverride
      include Pgbus::StreamsHelper

      def turbo_stream_from(*streamables, **attributes)
        if Pgbus.configuration.streams_enabled
          pgbus_stream_from(*streamables, **attributes)
        else
          super
        end
      end
    end

    # Apply the patch to Turbo::StreamsHelper. Idempotent: prepending the
    # same module twice is a no-op. Called from Pgbus::Engine's initializer
    # when both turbo-rails and pgbus streams are enabled.
    def self.install_turbo_stream_override!
      return unless defined?(::Turbo::StreamsHelper)
      return if ::Turbo::StreamsHelper.ancestors.include?(TurboStreamOverride)

      ::Turbo::StreamsHelper.prepend(TurboStreamOverride)
    end
  end
end
