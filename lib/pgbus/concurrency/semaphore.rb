# frozen_string_literal: true

module Pgbus
  module Concurrency
    module Semaphore
      class << self
        # Attempt to acquire a slot in the semaphore for the given key.
        # Returns :acquired if a slot was available, :blocked if the limit is reached.
        def acquire(key, max_value, duration)
          expires_at = Time.current + duration
          Pgbus::Semaphore.acquire!(key, max_value, expires_at)
        end

        # Release one slot in the semaphore. Called after a job completes.
        def release(key)
          Pgbus::Semaphore.where(key: key).update_all("value = GREATEST(value - 1, 0)")
        end

        # Delete semaphores that have expired (safety net for crashed workers).
        # Returns an array of hashes with expired keys.
        # Uses DELETE ... RETURNING for atomicity (no race between pluck and delete).
        def expire_stale
          result = Pgbus::Semaphore.connection.exec_query(
            "DELETE FROM pgbus_semaphores WHERE expires_at < $1 RETURNING key",
            "Pgbus Semaphore Expire",
            [Time.current]
          )
          result.rows.map { |row| { "key" => row[0] } }
        end

        # Check current value for a key. Useful for testing/monitoring.
        def current_value(key)
          Pgbus::Semaphore.where(key: key).pick(:value)
        end
      end
    end
  end
end
