# frozen_string_literal: true

module Pgbus
  class Configuration
    # Parses the capsule string DSL into the internal worker config array.
    #
    # The DSL lets users describe worker thread pools (capsules) compactly:
    #
    #   "*: 5"                                # one capsule, all queues, 5 threads
    #   "critical: 5; default: 10"            # two capsules
    #   "critical, default: 5"                # one capsule, list = strict priority
    #   "default, mailers: 10; *: 2"          # specialized + fallback wildcard
    #   "staging_*: 3"                        # prefix wildcard
    #
    # Operators:
    #   ,    queue separator within a capsule (list order = strict priority)
    #   ;    capsule separator (each becomes its own thread pool)
    #   :N   thread count for the capsule (default 5)
    #   *    wildcard, matches all queues
    #   *_   trailing wildcard, prefix match (e.g. "staging_*")
    #
    # Returns +Array<Hash>+ in the same shape as the legacy +workers:+ array,
    # so the rest of the codebase can consume it without changes:
    #
    #   [{ queues: ["critical"], threads: 5 },
    #    { queues: ["default", "mailers"], threads: 10 }]
    #
    # Validation runs at parse time. Errors include the offending input and
    # a clear message naming the rule that was violated.
    class CapsuleDSL
      DEFAULT_THREADS = 5
      WILDCARD = "*"

      # Valid queue tokens accepted by the DSL:
      #   "*"         bare wildcard (matches all queues at runtime)
      #   "default"   bare queue name
      #   "staging_*" prefix wildcard (matches queues by prefix at runtime)
      #
      # The character class for queue names — alphanumerics and underscores
      # only — intentionally matches Pgbus::QueueNameValidator::VALID_QUEUE_NAME_PATTERN.
      # Hyphens, dots, and other punctuation are NOT permitted because PGMQ
      # interpolates queue names directly into SQL identifiers (q_<name>,
      # a_<name>) and only those characters are safe there. If you change
      # this pattern, also update QueueNameValidator to keep them in sync.
      #
      # The "*" tokens (bare and trailing) are DSL-only — they get expanded
      # to concrete queue names at runtime before reaching QueueNameValidator.
      QUEUE_NAME_PATTERN = /\A(?:\*|[a-zA-Z0-9_]+\*?)\z/
      THREAD_COUNT_PATTERN = /\A\d+\z/

      class ParseError < ArgumentError; end

      def self.parse(input)
        new(input).parse
      end

      def initialize(input)
        @input = input
      end

      def parse
        validate_input_type!
        validate_input_not_empty!

        # Pure tokenization: split, parse each capsule, return them in order.
        # Cross-capsule overlap rules live in Pgbus::Configuration#workers=
        # because they depend on whether the resulting capsules are named or
        # anonymous, and naming is a Configuration concern (not a parser one).
        # Within-capsule duplicate-queue checks still happen in parse_capsule.
        split_capsules(@input).map { |segment| parse_capsule(segment) }
      end

      private

      def validate_input_type!
        return if @input.is_a?(String)

        raise ParseError,
              "expected String, got #{@input.class} (#{@input.inspect})"
      end

      def validate_input_not_empty!
        return if @input.strip != ""

        raise ParseError, "empty capsule string — must declare at least one queue"
      end

      # Splits on `;` and trims whitespace, dropping a trailing empty segment
      # so `"a; b;"` is treated the same as `"a; b"`. Leading-empty segments
      # ("; a") are kept and rejected by parse_capsule with a clearer error.
      def split_capsules(string)
        segments = string.strip.split(";").map(&:strip)
        segments.pop if segments.last == "" # trailing semicolon
        segments
      end

      def parse_capsule(segment)
        if segment == ""
          raise ParseError,
                "empty capsule in #{@input.inspect} — leading or doubled semicolons are not allowed"
        end

        queues_part, threads_part = split_on_threads(segment)
        queues = parse_queue_list(queues_part, segment)
        threads = parse_thread_count(threads_part, segment)
        validate_no_duplicates_within_capsule!(queues, segment)

        { queues: queues, threads: threads }
      end

      # Splits a capsule segment on the LAST `:` so queue names containing
      # underscores or digits are unaffected. Returns [queues_part, threads_part]
      # where threads_part is nil when no `:` is present.
      def split_on_threads(segment)
        idx = segment.rindex(":")
        return [segment, nil] unless idx

        [segment[0...idx].strip, segment[(idx + 1)..].strip]
      end

      def parse_queue_list(queues_part, original_segment)
        if queues_part.empty?
          raise ParseError,
                "expected queue name before ':' in #{original_segment.inspect}"
        end

        queues = queues_part.split(",").map(&:strip)
        queues.each { |q| validate_queue_name!(q, original_segment) }
        queues
      end

      def validate_queue_name!(name, original_segment)
        return if name.match?(QUEUE_NAME_PATTERN)

        raise ParseError,
              "invalid character in queue name #{name.inspect} (in #{original_segment.inspect}) — " \
              "queue names must match /[a-zA-Z0-9_]+\\*?/"
      end

      def parse_thread_count(threads_part, original_segment)
        return DEFAULT_THREADS if threads_part.nil?

        if threads_part.empty?
          raise ParseError,
                "expected thread count after ':' in #{original_segment.inspect}"
        end

        unless threads_part.match?(THREAD_COUNT_PATTERN)
          raise ParseError,
                "invalid thread count #{threads_part.inspect} in #{original_segment.inspect} — " \
                "must be a positive integer"
        end

        n = threads_part.to_i
        if n.zero?
          raise ParseError,
                "thread count must be a positive integer, got 0 in #{original_segment.inspect}"
        end

        n
      end

      def validate_no_duplicates_within_capsule!(queues, original_segment)
        seen = {}
        queues.each do |q|
          if seen[q]
            raise ParseError,
                  "queue #{q.inspect} listed twice in capsule #{original_segment.inspect} — " \
                  "duplicate queues within a capsule are not allowed"
          end
          seen[q] = true
        end
      end
    end
  end
end
