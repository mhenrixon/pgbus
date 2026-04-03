# frozen_string_literal: true

module Pgbus
  class Semaphore < BusRecord
    self.table_name = "pgbus_semaphores"

    scope :expired, ->(now = Time.current) { where("expires_at < ? OR value <= 0", now) }

    # Atomic conditional UPSERT. Returns :acquired or :blocked.
    def self.acquire!(key, max_value, expires_at)
      result = connection.exec_query(
        <<~SQL,
          INSERT INTO pgbus_semaphores (key, value, max_value, expires_at)
          VALUES ($1, 1, $2, $3)
          ON CONFLICT (key) DO UPDATE
            SET value = pgbus_semaphores.value + 1,
                max_value = EXCLUDED.max_value,
                expires_at = GREATEST(pgbus_semaphores.expires_at, EXCLUDED.expires_at)
            WHERE pgbus_semaphores.value < EXCLUDED.max_value
          RETURNING value
        SQL
        "Pgbus Semaphore Acquire",
        [key, max_value, expires_at]
      )

      result.any? ? :acquired : :blocked
    end
  end
end
