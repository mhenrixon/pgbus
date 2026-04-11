# frozen_string_literal: true

module Pgbus
  module Streams
    # ActiveRecord concern that adds `short_id` and `to_stream_key`
    # instance methods for producing pgbus-safe stream identifiers from
    # records whose primary key is a UUID (or any other long string).
    #
    # Intended for inclusion in `ApplicationRecord`:
    #
    #     class ApplicationRecord < ActiveRecord::Base
    #       primary_abstract_class
    #       include Pgbus::Streams::Streamable
    #     end
    #
    # Every subclass then gets:
    #
    #     chat.short_id
    #     # => "3a4f9c21b7d20e18"
    #
    #     chat.to_stream_key
    #     # => "ai_chat_3a4f9c21b7d20e18"
    #
    #     Pgbus.stream_key(chat, :messages)
    #     # => "ai_chat_3a4f9c21b7d20e18:messages"
    #
    # The mixin is intentionally thin: it delegates to `Pgbus::Streams::Key`
    # so the digest policy lives in one place. Host apps that already
    # define `#short_id` will collide at include time — that's loud by
    # design; silent override would hide a real conflict.
    module Streamable
      # Returns a short SHA-256 prefix (64 bits / 16 hex chars by default)
      # of this record's primary key. See `Pgbus::Streams::Key.short_id`
      # for the digest policy and collision horizon.
      def short_id(digest_bits: Key::DEFAULT_DIGEST_BITS)
        Key.short_id(self, digest_bits: digest_bits)
      end

      # Returns a stable, pgbus-safe identifier of the form
      # `<model_key>_<short_id>` suitable for passing directly to
      # `Pgbus.stream(...)` or composing with `Pgbus.stream_key`.
      def to_stream_key
        "#{self.class.model_name.param_key}_#{short_id}"
      end
    end
  end
end
