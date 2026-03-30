# frozen_string_literal: true

module Pgbus
  module Concurrency
    module Semaphore
      class << self
        # Attempt to acquire a slot in the semaphore for the given key.
        # Returns :acquired if a slot was available, :blocked if the limit is reached.
        def acquire(key, max_value, duration)
          expires_at = Time.now.utc + duration

          result = execute(<<~SQL, "Pgbus Semaphore Acquire", [key, max_value, expires_at])
            INSERT INTO pgbus_semaphores (key, value, max_value, expires_at)
            VALUES ($1, 1, $2, $3)
            ON CONFLICT (key) DO UPDATE
              SET value = pgbus_semaphores.value + 1,
                  max_value = EXCLUDED.max_value,
                  expires_at = GREATEST(pgbus_semaphores.expires_at, EXCLUDED.expires_at)
              WHERE pgbus_semaphores.value < pgbus_semaphores.max_value
            RETURNING value
          SQL

          result.any? ? :acquired : :blocked
        end

        # Release one slot in the semaphore. Called after a job completes.
        def release(key)
          execute(<<~SQL, "Pgbus Semaphore Release", [key])
            UPDATE pgbus_semaphores
            SET value = GREATEST(value - 1, 0)
            WHERE key = $1
          SQL
        end

        # Delete semaphores that have expired (safety net for crashed workers).
        # Returns the number of deleted rows.
        def expire_stale
          result = execute(<<~SQL, "Pgbus Semaphore Expire", [Time.now.utc])
            DELETE FROM pgbus_semaphores
            WHERE expires_at < $1 OR value <= 0
            RETURNING key
          SQL

          result.to_a
        end

        # Check current value for a key. Useful for testing/monitoring.
        def current_value(key)
          result = execute(<<~SQL, "Pgbus Semaphore Value", [key])
            SELECT value FROM pgbus_semaphores WHERE key = $1
          SQL

          result.first&.fetch("value", nil)&.to_i
        end

        private

        def execute(sql, name, binds)
          return [] unless defined?(ActiveRecord::Base)

          ActiveRecord::Base.connection.exec_query(sql, name, binds)
        end
      end
    end
  end
end
