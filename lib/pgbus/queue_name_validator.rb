# frozen_string_literal: true

module Pgbus
  # Validates and sanitizes PGMQ queue names for safe use in SQL identifiers.
  #
  # PGMQ queue names are interpolated into SQL as table/sequence names
  # (e.g., pgmq.q_<name>, pgmq.a_<name>). This module enforces strict
  # validation to prevent SQL injection via crafted queue names.
  module QueueNameValidator
    # PostgreSQL identifier limit is 63 bytes (NAMEDATALEN - 1).
    # PGMQ prefixes with "q_" or "a_" (2 chars), so limit the name itself.
    MAX_QUEUE_NAME_LENGTH = 61

    # Only alphanumeric characters and underscores are allowed.
    VALID_QUEUE_NAME_PATTERN = /\A[a-zA-Z0-9_]+\z/

    module_function

    # Validates a queue name for safe SQL identifier use.
    # Returns the name if valid, raises ArgumentError if not.
    def validate!(name)
      name = name.to_s
      raise ArgumentError, "Queue name cannot be blank" if name.empty?
      if name.length > MAX_QUEUE_NAME_LENGTH
        raise ArgumentError,
              "Queue name too long (#{name.length} chars, max #{MAX_QUEUE_NAME_LENGTH}): #{name.inspect}"
      end

      unless VALID_QUEUE_NAME_PATTERN.match?(name)
        raise ArgumentError,
              "Invalid queue name: #{name.inspect}. Only alphanumeric characters and underscores are allowed."
      end

      name
    end

    # Normalizes a queue name by replacing common separators (hyphens, dots)
    # with underscores, stripping remaining invalid characters, and collapsing
    # consecutive underscores. Use this for names from external sources
    # (e.g., Turbo stream names like "hotwire-livereload") where the intent
    # is to derive a valid PGMQ queue name that preserves readability.
    def normalize(name)
      name = name.to_s
      return validate!(name) if VALID_QUEUE_NAME_PATTERN.match?(name)

      normalized = name.gsub(/[-.]/, "_")           # hyphens/dots → underscores
                       .gsub(/[^a-zA-Z0-9_]/, "")   # strip remaining invalid chars
                       .gsub(/_+/, "_")              # collapse consecutive underscores
                       .gsub(/\A_|_\z/, "")          # strip leading/trailing underscores
      validate!(normalized)
      normalized
    end

    # Sanitizes a queue name by removing invalid characters, then validates.
    # Use this for names from untrusted sources (e.g., URL params).
    def sanitize!(name)
      sanitized = name.to_s.gsub(/[^a-zA-Z0-9_]/, "")
      validate!(sanitized)
      sanitized
    end
  end
end
