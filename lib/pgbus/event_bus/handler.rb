# frozen_string_literal: true

module Pgbus
  module EventBus
    class Handler
      class << self
        def idempotent!
          @idempotent = true
        end

        def idempotent?
          @idempotent == true
        end

        def dedup_cache
          @dedup_cache ||= DedupCache.new
        end
      end

      def process(message)
        with_rails_executor { process!(message) }
      end

      def handle(event)
        raise NotImplementedError, "#{self.class.name} must implement #handle(event)"
      end

      private

      def process!(message)
        raw = JSON.parse(message.message)
        event = build_event(raw)

        return :skipped if self.class.idempotent? && !claim_idempotency?(event.event_id)

        handle(event)
        instrument("pgbus.event_processed", event_id: event.event_id, handler: self.class.name)
        :handled
      end

      # Mirrors Pgbus::ActiveJob::Executor#execute_job: wrap the handler
      # invocation in Rails.application.executor (or the reloader in dev)
      # so AR connections leased by `claim_idempotency?` and `handle` are
      # released back to the pool when this method returns. Without the
      # wrap, every consumed event leaks one AR connection on the consumer
      # thread — in dev that wedges `clear_reloadable_connections!`,
      # producing a confusing Rack::Timeout in `MonitorMixin#wait_for_cond`.
      #
      # No-op when Rails isn't loaded (test harnesses, gem-only consumers).
      def with_rails_executor(&)
        return yield unless defined?(Rails) && Rails.respond_to?(:application) && Rails.application

        wrapper = reloading? ? Rails.application.reloader : Rails.application.executor
        wrapper.wrap(&)
      end

      def reloading?
        app_config = Rails.application.config
        if app_config.respond_to?(:enable_reloading)
          app_config.enable_reloading
        else
          !app_config.cache_classes
        end
      end

      def build_event(raw)
        payload = raw["payload"]
        payload = Serializer.locate_global_id(payload["_global_id"]) if payload.is_a?(Hash) && payload["_global_id"]

        Event.new(
          event_id: raw["event_id"],
          payload: payload,
          published_at: raw["published_at"] ? Time.parse(raw["published_at"]) : nil
        )
      end

      def instrument(event_name, payload = {})
        return unless defined?(ActiveSupport::Notifications)

        ActiveSupport::Notifications.instrument(event_name, payload)
      end

      # Atomically claim idempotency: INSERT ... ON CONFLICT DO NOTHING.
      # Returns true if this handler claimed the event (row was inserted),
      # false if another handler already processed it (conflict, no insert).
      #
      # Uses an in-memory dedup cache to skip the DB for recently-seen events.
      def claim_idempotency?(event_id)
        cache_key = "#{event_id}:#{self.class.name}"
        return false if self.class.dedup_cache.seen?(cache_key)

        result = ProcessedEvent.insert(
          { event_id: event_id, handler_class: self.class.name, processed_at: Time.now.utc },
          unique_by: %i[event_id handler_class]
        )

        claimed = result.rows.any?
        self.class.dedup_cache.mark!(cache_key)
        claimed
      end
    end
  end
end
