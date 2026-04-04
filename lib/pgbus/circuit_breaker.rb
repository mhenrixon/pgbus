# frozen_string_literal: true

require "concurrent"

module Pgbus
  class CircuitBreaker
    attr_reader :config

    def initialize(config: Pgbus.configuration)
      @config = config
      @failure_counts = Concurrent::Map.new
      @pause_cache = Concurrent::Map.new
      @pause_cache_ttl = 30 # seconds
    end

    def record_success(queue_name)
      @failure_counts.delete(queue_name)
    end

    def record_failure(queue_name)
      return unless config.circuit_breaker_enabled

      count = @failure_counts.compute(queue_name) { |val| (val || 0) + 1 }

      return unless count >= config.circuit_breaker_threshold

      trip!(queue_name, count)
    end

    def paused?(queue_name)
      cached = @pause_cache[queue_name]
      return cached[:paused] if cached && (Time.current - cached[:checked_at]) < @pause_cache_ttl

      paused = check_paused(queue_name)
      @pause_cache[queue_name] = { paused: paused, checked_at: Time.current }
      paused
    end

    def pause!(queue_name, reason: nil)
      QueueState.pause!(queue_name, reason: reason)
      invalidate_cache(queue_name)
    end

    def resume!(queue_name)
      QueueState.resume!(queue_name)
      @failure_counts.delete(queue_name)
      invalidate_cache(queue_name)
    end

    def invalidate_cache(queue_name = nil)
      if queue_name
        @pause_cache.delete(queue_name)
      else
        @pause_cache.clear
      end
    end

    private

    def trip!(queue_name, failure_count)
      trip_count = current_trip_count(queue_name) + 1
      backoff = calculate_backoff(trip_count)
      resume_at = Time.current + backoff

      Pgbus.logger.warn do
        "[Pgbus] Circuit breaker tripped for #{queue_name}: #{failure_count} consecutive failures, " \
          "backoff #{backoff}s (trip ##{trip_count})"
      end

      QueueState.find_or_initialize_by(queue_name: queue_name).update!(
        paused: true,
        paused_reason: "circuit_breaker: #{failure_count} consecutive failures",
        paused_at: Time.current,
        circuit_breaker_trip_count: trip_count,
        circuit_breaker_resume_at: resume_at
      )

      @failure_counts.delete(queue_name)
      invalidate_cache(queue_name)
    rescue StandardError => e
      Pgbus.logger.error { "[Pgbus] Circuit breaker trip failed for #{queue_name}: #{e.message}" }
    end

    def check_paused(queue_name)
      state = QueueState.find_by(queue_name: queue_name)
      return false unless state&.paused?

      # Auto-resume if circuit breaker backoff has expired
      if state.circuit_breaker_resume_at && Time.current >= state.circuit_breaker_resume_at
        QueueState.resume!(queue_name)
        Pgbus.logger.info { "[Pgbus] Circuit breaker auto-resumed #{queue_name}" }
        return false
      end

      true
    rescue StandardError => e
      Pgbus.logger.warn { "[Pgbus] Circuit breaker pause check failed for #{queue_name}: #{e.message}" }
      false
    end

    def current_trip_count(queue_name)
      QueueState.find_by(queue_name: queue_name)&.circuit_breaker_trip_count || 0
    rescue StandardError
      0
    end

    def calculate_backoff(trip_count)
      backoff = config.circuit_breaker_base_backoff * (2**(trip_count - 1))
      [backoff, config.circuit_breaker_max_backoff].min
    end
  end
end
