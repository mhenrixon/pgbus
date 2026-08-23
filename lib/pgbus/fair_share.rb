# frozen_string_literal: true

module Pgbus
  # Fair share scheduling across tenants (issue #426).
  #
  # At enqueue time the configured callable (`config.fair_share`) resolves a
  # key — typically a tenant id — and an optional weight for the job. Both
  # ride inside the job payload hash (same pattern as Concurrency::METADATA_KEY
  # and Batch::METADATA_KEY), so they survive every path that re-sends a
  # payload: blocked-execution promotion, DLQ retry, dashboard retry, bulk
  # enqueue. On the read side Client#read_batch_fair interleaves across keys
  # proportionally to weight (see Client::FairRead).
  #
  # Events (issue #427) use the same keys and the same read primitive: the
  # configured `config.event_fair_share` callable receives the Pgbus::Event at
  # publish time and the key rides in the event *envelope* (a sibling of
  # event_id / payload / published_at — never inside the user payload), so the
  # same `message->>'pgbus_fair_key'` expression, index and read SQL serve both
  # jobs and events, and the outbox (which stores the envelope) carries it.
  module FairShare
    METADATA_KEY = "pgbus_fair_key"
    WEIGHT_KEY = "pgbus_fair_weight"
    DEFAULT_WEIGHT = 1

    class << self
      def enabled?(config = Pgbus.configuration)
        !config.fair_share.nil?
      end

      def event_enabled?(config = Pgbus.configuration)
        !config.event_fair_share.nil?
      end

      # Returns the payload with fair-share metadata merged in, or the payload
      # itself (same object) when fair share is off or the job is unkeyed.
      # Exceptions raised by the callable propagate — a key resolver that
      # cannot run is a programmer error, not something to swallow at enqueue.
      def inject_metadata(active_job, payload_hash, config = Pgbus.configuration)
        return payload_hash unless enabled?(config)

        tag(payload_hash, resolve(active_job, config))
      end

      # Event twin of inject_metadata: returns the event envelope (the hash
      # Publisher.build_event_data produced) with fair-share metadata merged
      # in, or the envelope itself (same object) when event fair share is off
      # or the callable declines to key the event.
      def inject_event_metadata(event, event_data, config = Pgbus.configuration)
        return event_data unless event_enabled?(config)

        tag(event_data, resolve_event(event, config))
      end

      # [key, weight] for the job, or nil when the callable declines to key it.
      def resolve(active_job, config = Pgbus.configuration)
        normalize(config.fair_share.call(active_job))
      end

      # [key, weight] for the event, or nil when the callable declines.
      def resolve_event(event, config = Pgbus.configuration)
        normalize(config.event_fair_share.call(event))
      end

      def extract_key(payload)
        payload[METADATA_KEY]
      end

      def extract_weight(payload)
        payload[WEIGHT_KEY] || DEFAULT_WEIGHT
      end

      private

      def tag(hash, resolved)
        return hash unless resolved

        key, weight = resolved
        tagged = hash.merge(METADATA_KEY => key)
        tagged[WEIGHT_KEY] = weight unless weight == DEFAULT_WEIGHT
        tagged
      end

      def normalize(result)
        return nil if result.nil?

        raw_key, raw_weight = result.is_a?(Array) ? result : [result, nil]
        [normalize_key(raw_key), normalize_weight(raw_weight)]
      end

      def normalize_key(raw)
        key = case raw
              when String, Symbol, Integer then raw.to_s
              else
                raise ArgumentError,
                      "fair_share key must be a String, Symbol, or Integer (got #{raw.class})"
              end
        raise ArgumentError, "fair_share key must not be empty" if key.empty?

        key
      end

      def normalize_weight(raw)
        return DEFAULT_WEIGHT if raw.nil?
        return raw if raw.is_a?(Numeric) && raw.positive?

        raise ArgumentError, "fair_share weight must be a positive Numeric (got #{raw.inspect})"
      end
    end
  end
end
