# frozen_string_literal: true

require "concurrent"

module Pgbus
  # Thread-safe in-memory dedup cache with TTL.
  # Sits in front of the pgbus_processed_events table to avoid
  # hitting the database for recently-seen (event_id, handler_class) pairs.
  #
  # Inspired by LavinMQ's built-in message deduplication with LRU + TTL.
  class DedupCache
    DEFAULT_MAX_SIZE = 10_000
    DEFAULT_TTL = 300 # 5 minutes

    def initialize(max_size: DEFAULT_MAX_SIZE, ttl: DEFAULT_TTL)
      @max_size = max_size
      @ttl = ttl
      @cache = Concurrent::Map.new
      @insertion_order = Concurrent::Array.new
    end

    # Returns true if this key was already seen (duplicate).
    # Returns false if it's new (first time seen — caller should proceed).
    def seen?(key)
      entry = @cache[key]
      return false unless entry

      if expired?(entry)
        evict(key)
        return false
      end

      true
    end

    # Mark a key as seen. Call this after successfully claiming idempotency
    # in the database so future lookups skip the DB.
    def mark!(key)
      evict_oldest if @cache.size >= @max_size

      @cache[key] = monotonic_now
      @insertion_order << key
    end

    def size
      @cache.size
    end

    def clear!
      @cache.clear
      @insertion_order.clear
    end

    private

    def expired?(timestamp)
      (monotonic_now - timestamp) > @ttl
    end

    def evict(key)
      @cache.delete(key)
    end

    def evict_oldest
      while @cache.size >= @max_size && !@insertion_order.empty?
        oldest_key = @insertion_order.shift
        @cache.delete(oldest_key)
      end
    end

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
