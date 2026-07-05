# frozen_string_literal: true

module Pgbus
  module Streams
    # Runtime patch that redirects `Turbo::StreamsChannel.broadcast_stream_to`
    # through pgbus instead of `ActionCable.server.broadcast`. Applied at
    # Rails engine boot time when `defined?(::Turbo::StreamsChannel)` —
    # see `Pgbus::Engine`'s initializer. When turbo-rails isn't loaded,
    # this patch is a no-op and pgbus streams continue to work via the
    # explicit `Pgbus.stream(...).broadcast(...)` API.
    #
    # After the patch:
    #
    #   class Order < ApplicationRecord
    #     broadcasts_to :account   # existing turbo-rails API, unchanged
    #   end
    #
    #   # In a controller:
    #   @order.update!(status: "shipped")
    #   # → Turbo::Broadcastable runs its after_update_commit callback
    #   # → calls Turbo::StreamsChannel.broadcast_replace_to
    #   # → which calls Turbo::StreamsChannel.broadcast_stream_to
    #   # → which is patched to call Pgbus.stream(name).broadcast(content)
    #   # → which inserts into PGMQ and fires NOTIFY
    #
    # Zero changes to user code. The entire Turbo::Broadcastable API
    # (broadcasts_to, broadcasts_refreshes, broadcast_replace_to,
    # broadcast_append_later_to, broadcasts_refreshes_to, etc) reuses
    # this code path because they all funnel through broadcast_stream_to.
    #
    # Signed stream name reuse: we don't touch `Turbo.signed_stream_verifier`,
    # so any existing `broadcasts_to :room` call continues to generate
    # tokens that our `Pgbus::Streams::SignedName.verify!` accepts (as
    # long as `Turbo.signed_stream_verifier_key` is set, which the Rails
    # app is already responsible for).
    module TurboBroadcastable
      def broadcast_stream_to(*streamables, content:)
        name = stream_name_from(streamables)
        override = Thread.current[:pgbus_broadcast_durable]
        # When no explicit thread-local override is present, let the config
        # resolver decide: it checks `streams_durable_patterns` first (exact
        # string or regex match), then falls back to
        # `streams_default_broadcast_mode`. Passing this through keeps
        # pattern-based routing alive for the whole Turbo::Broadcastable and
        # phlex-reactive broadcast path (see issue #267).
        durable = override.nil? ? Pgbus.configuration.stream_durable?(name) : override
        Pgbus.stream(name, durable: durable).broadcast(
          content,
          exclude: Thread.current[:pgbus_broadcast_exclude],
          visible_to: Thread.current[:pgbus_broadcast_visible_to],
          event: Thread.current[:pgbus_broadcast_event]
        )
      end
    end

    # Apply the patch to Turbo::StreamsChannel's singleton class. Idempotent:
    # prepending the same module twice is a no-op. Called from
    # Pgbus::Engine's initializer when Turbo is detected.
    def self.install_turbo_broadcastable_patch!
      return unless defined?(::Turbo::StreamsChannel)
      return if ::Turbo::StreamsChannel.singleton_class.include?(TurboBroadcastable)

      ::Turbo::StreamsChannel.singleton_class.prepend(TurboBroadcastable)
    end

    # turbo-rails' async broadcast helpers (`broadcast_*_later_to`, the
    # default for `broadcasts_to`/`broadcasts_refreshes`) enqueue these three
    # ActiveJobs, which ship with no `queue_as` and so land on the default
    # queue — where a render+broadcast can wait behind long-running jobs
    # before the browser sees the update (#311). When the operator sets
    # `config.streams_broadcast_queue`, route them to that dedicated queue so
    # a `realtime:`-style worker capsule can isolate broadcast latency from job
    # throughput. Called from the engine's turbo_broadcastable initializer.
    # No-op when the queue is nil or turbo-rails is not loaded.
    def self.install_broadcast_queue!(queue_name)
      return if queue_name.nil?

      %w[
        Turbo::Streams::ActionBroadcastJob
        Turbo::Streams::BroadcastJob
        Turbo::Streams::BroadcastStreamJob
      ].each do |const_name|
        next unless Object.const_defined?(const_name)

        Object.const_get(const_name).queue_as(queue_name)
      end
    end
  end
end
