# frozen_string_literal: true

require "concurrent"

module Pgbus
  # Thread-safe rate counter inspired by LavinMQ's rate_stats macro.
  # Tracks absolute counts via AtomicFixnum and computes rates as
  # deltas between periodic snapshots.
  #
  # Usage:
  #   counter = RateCounter.new(:enqueued, :dequeued, :failed)
  #   counter.increment(:enqueued)
  #   counter.snapshot!          # call periodically (e.g. every 5s)
  #   counter.rate(:enqueued)    # => msgs/s since last snapshot
  #   counter.rates              # => { enqueued: 12.4, dequeued: 10.1, failed: 0.2 }
  class RateCounter
    attr_reader :names

    def initialize(*names)
      @names = names.map(&:to_sym)
      @counters = {}
      @last_values = {}
      @rates = {}
      @last_snapshot_at = monotonic_now

      @names.each do |name|
        @counters[name] = Concurrent::AtomicFixnum.new(0)
        @last_values[name] = 0
        @rates[name] = 0.0
      end
    end

    def increment(name, delta = 1)
      @counters.fetch(name).increment(delta)
    end

    def count(name)
      @counters.fetch(name).value
    end

    def rate(name)
      @rates.fetch(name)
    end

    def rates
      @names.to_h { |name| [name, @rates[name]] }
    end

    def counts
      @names.to_h { |name| [name, @counters[name].value] }
    end

    def snapshot!
      now = monotonic_now
      elapsed = now - @last_snapshot_at
      return if elapsed <= 0

      @names.each do |name|
        current = @counters[name].value
        delta = current - @last_values[name]
        @rates[name] = (delta / elapsed).round(1)
        @last_values[name] = current
      end

      @last_snapshot_at = now
    end

    def to_h
      {
        counts: counts,
        rates: rates
      }
    end

    private

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
