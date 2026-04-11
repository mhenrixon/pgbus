# frozen_string_literal: true

require "digest"

module Pgbus
  module Streams
    # Short, pgbus-safe stream identifiers.
    #
    # PGMQ queue names are subject to PostgreSQL's NAMEDATALEN ceiling
    # (63 chars for `pgmq.q_<name>`), and pgbus reserves two of those
    # for PGMQ's own `q_`/`a_` prefix — leaving 61 chars (see
    # QueueNameValidator::MAX_QUEUE_NAME_LENGTH). Any stream name composed
    # from UUID primary keys and turbo-rails-style dom ids blows past that
    # budget almost immediately:
    #
    #     "gid://app/Ai::Chat/9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca:messages"
    #     # => 63 chars, already too long before the "pgbus_" prefix is added.
    #
    # `stream_key` produces a deterministic short form suitable as a
    # pgbus stream identifier. It normalizes each part, joins with ":",
    # and enforces the queue-name budget (derived from the configured
    # `queue_prefix`) at the call site — raising ArgumentError rather
    # than letting the failure surface as an opaque QueueNameValidator
    # error deep inside `Pgbus.stream(...).broadcast(...)`.
    #
    # Silent truncation is intentionally NOT supported: trimming a
    # too-long key to fit would reintroduce the collision risk that the
    # 64-bit digest is chosen to eliminate. Callers who overflow should
    # shorten their own identifiers or adopt `Pgbus::Streams::Streamable`
    # on their ActiveRecord models.
    #
    # Usage:
    #
    #     Pgbus.stream_key(chat, :messages)
    #       # => "ai_chat_3a4f9c21b7d20e18:messages"
    #
    #     Pgbus.stream_key([user, :notifications])
    #       # => "user_5fa83c91d44a2701:notifications"
    #
    #     Pgbus.stream(Pgbus.stream_key(chat, :messages)).broadcast("<turbo-stream/>")
    #
    # Collision horizon: the 64-bit SHA-256 prefix gives a birthday bound
    # of roughly 5 billion records per model class before a 50% chance of
    # collision. For multi-tenant apps where a collision would mean two
    # records share a stream (and receive each other's broadcasts), this
    # is wide enough in practice. Callers with higher sensitivity can
    # pass `digest_bits: 128`.
    module Key
      DEFAULT_DIGEST_BITS = 64

      module_function

      # Compose a short pgbus-safe stream name from any mix of records,
      # strings, symbols, and arrays. Returns the joined key when it fits
      # the pgbus queue-name budget; raises ArgumentError otherwise.
      def stream_key(*parts, digest_bits: DEFAULT_DIGEST_BITS)
        key = Array(parts).flatten.map { |part| normalize(part, digest_bits: digest_bits) }.join(":")
        budget = queue_name_budget
        return key if key.length <= budget

        raise ArgumentError,
              "stream_key #{key.inspect} is #{key.length} chars, " \
              "exceeds pgbus budget of #{budget} " \
              "(queue_prefix=#{Pgbus.configuration.queue_prefix.inspect}, " \
              "NAMEDATALEN=#{QueueNameValidator::MAX_QUEUE_NAME_LENGTH}). " \
              "Shorten the streamables or use Pgbus::Streams::Streamable on the model."
      end

      # 64-bit (default) SHA-256 prefix of the record's primary key. Stdlib
      # only, deterministic, and fixed-length. CRC32's 32-bit output is
      # intentionally not used here: its ~77k-row birthday bound is too
      # tight for a multi-tenant stream identifier where a collision would
      # route two records' broadcasts to the same queue.
      def short_id(record, digest_bits: DEFAULT_DIGEST_BITS)
        unless digest_bits.is_a?(Integer) && digest_bits.positive? && (digest_bits % 4).zero?
          raise ArgumentError, "digest_bits must be a positive multiple of 4"
        end

        hex_chars = digest_bits / 4
        ::Digest::SHA256.hexdigest(record.id.to_s)[0, hex_chars]
      end

      # Normalize a single streamable fragment to a pgbus-safe string.
      # Mirrors the shape accepted by Turbo::Streams::StreamName and
      # Pgbus::Streams::Stream.name_from so the two code paths agree
      # on the wire format.
      #
      # - Strings and symbols pass through verbatim.
      # - ActiveRecord models become "<param_key>_<short_id>".
      # - Anything else responding to `to_gid_param` / `to_param` falls
      #   back to that; a UUID primary key would still overflow, which
      #   is why AR models are hashed above.
      def normalize(part, digest_bits: DEFAULT_DIGEST_BITS)
        case part
        when String, Symbol
          part.to_s
        else
          if defined?(::ActiveRecord::Base) && part.is_a?(::ActiveRecord::Base)
            "#{part.class.model_name.param_key}_#{short_id(part, digest_bits: digest_bits)}"
          elsif part.respond_to?(:to_stream_key)
            part.to_stream_key
          elsif part.respond_to?(:to_gid_param)
            part.to_gid_param
          elsif part.respond_to?(:to_param)
            part.to_param
          else
            part.to_s
          end
        end
      end

      # Budget = NAMEDATALEN ceiling - "<queue_prefix>_" length.
      # Computed at call time (not a constant) so apps that override
      # `config.queue_prefix` get the correct budget automatically.
      def queue_name_budget
        QueueNameValidator::MAX_QUEUE_NAME_LENGTH -
          Pgbus.configuration.queue_prefix.length - 1
      end
    end
  end
end
