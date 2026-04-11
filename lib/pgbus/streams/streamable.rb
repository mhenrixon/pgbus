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
    # so the digest policy lives in one place.
    #
    # `#to_stream_key` calls `Key.short_id(self)` directly rather than
    # dispatching through `#short_id`. Ruby does NOT warn when a class
    # defines an instance method and a later `include` adds a module
    # with the same name — the class method silently wins. A host app
    # that already defines its own `#short_id` (returning, say, a
    # display-friendly abbreviation) would therefore hijack
    # `to_stream_key` without any indication, producing stream keys
    # the wire format never promised. Calling `Key.short_id(self)`
    # explicitly bypasses instance-method lookup and guarantees the
    # advertised digest regardless of what the host class does with
    # the unqualified name.
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
        "#{self.class.model_name.param_key}_#{Key.short_id(self)}"
      end
    end
  end
end
