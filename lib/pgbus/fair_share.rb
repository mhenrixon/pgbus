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
  module FairShare
    METADATA_KEY = "pgbus_fair_key"
    WEIGHT_KEY = "pgbus_fair_weight"
    DEFAULT_WEIGHT = 1

    class << self
      def enabled?(config = Pgbus.configuration)
        !config.fair_share.nil?
      end

      # Returns the payload with fair-share metadata merged in, or the payload
      # itself (same object) when fair share is off or the job is unkeyed.
      # Exceptions raised by the callable propagate — a key resolver that
      # cannot run is a programmer error, not something to swallow at enqueue.
      def inject_metadata(active_job, payload_hash, config = Pgbus.configuration)
        return payload_hash unless enabled?(config)

        key, weight = resolve(active_job, config)
        return payload_hash unless key

        tagged = payload_hash.merge(METADATA_KEY => key)
        tagged[WEIGHT_KEY] = weight unless weight == DEFAULT_WEIGHT
        tagged
      end

      # [key, weight] for the job, or nil when the callable declines to key it.
      def resolve(active_job, config = Pgbus.configuration)
        result = config.fair_share.call(active_job)
        return nil if result.nil?

        raw_key, raw_weight = result.is_a?(Array) ? result : [result, nil]
        [normalize_key(raw_key), normalize_weight(raw_weight)]
      end

      def extract_key(payload)
        payload[METADATA_KEY]
      end

      def extract_weight(payload)
        payload[WEIGHT_KEY] || DEFAULT_WEIGHT
      end

      private

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
