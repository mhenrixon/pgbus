# frozen_string_literal: true

module Pgbus
  class BlockedExecution < ApplicationRecord
    self.table_name = "pgbus_blocked_executions"

    scope :for_key, ->(key) { where(concurrency_key: key) }
    scope :expired, ->(now = Time.current) { where("expires_at < ?", now) }

    # Atomic dequeue: DELETE the highest-priority non-expired row with FOR UPDATE SKIP LOCKED.
    # Returns { queue_name:, payload: } or nil.
    def self.release_next!(concurrency_key)
      now = Time.now.utc
      result = connection.exec_query(
        <<~SQL,
          DELETE FROM pgbus_blocked_executions
          WHERE id = (
            SELECT id FROM pgbus_blocked_executions
            WHERE concurrency_key = $1
              AND expires_at >= $2
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          )
          RETURNING queue_name, payload
        SQL
        "Pgbus Blocked Release",
        [concurrency_key, now]
      )

      row = result.first
      return nil unless row

      payload = row["payload"]
      payload = JSON.parse(payload) if payload.is_a?(String)

      { queue_name: row["queue_name"], payload: payload }
    end
  end
end
