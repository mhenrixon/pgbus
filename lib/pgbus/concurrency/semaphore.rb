# frozen_string_literal: true

module Pgbus
  module Concurrency
    module Semaphore
      class << self
        # Attempt to acquire a slot in the semaphore for the given key.
        # Returns :acquired if a slot was available, :blocked if the limit is reached.
        def acquire(key, max_value, duration)
          expires_at = Time.now.utc + duration
          SemaphoreRecord.acquire!(key, max_value, expires_at)
        end

        # Release one slot in the semaphore. Called after a job completes.
        def release(key)
          SemaphoreRecord.where(key: key).update_all("value = GREATEST(value - 1, 0)")
        end

        # Delete semaphores that have expired (safety net for crashed workers).
        # Returns an array of hashes with expired keys.
        def expire_stale
          expired = SemaphoreRecord.expired(Time.now.utc)
          keys = expired.pluck(:key)
          expired.delete_all
          keys.map { |k| { "key" => k } }
        end

        # Check current value for a key. Useful for testing/monitoring.
        def current_value(key)
          SemaphoreRecord.where(key: key).pick(:value)
        end
      end
    end
  end
end
