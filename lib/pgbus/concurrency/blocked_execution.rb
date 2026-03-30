# frozen_string_literal: true

require "json"

module Pgbus
  module Concurrency
    module BlockedExecution
      class << self
        # Insert a blocked execution for a job that hit the concurrency limit.
        def insert(concurrency_key:, queue_name:, payload:, duration:, priority: 0)
          expires_at = Time.now.utc + duration

          execute(<<~SQL, "Pgbus Blocked Insert", [concurrency_key, queue_name, JSON.generate(payload), priority, expires_at])
            INSERT INTO pgbus_blocked_executions
              (concurrency_key, queue_name, payload, priority, expires_at)
            VALUES ($1, $2, $3, $4, $5)
          SQL
        end

        # Release the next blocked execution for a given concurrency key.
        # Returns the released row (queue_name, payload) or nil if none.
        def release_next(concurrency_key)
          return nil unless defined?(ActiveRecord::Base)

          result = execute(<<~SQL, "Pgbus Blocked Release", [concurrency_key])
            DELETE FROM pgbus_blocked_executions
            WHERE id = (
              SELECT id FROM pgbus_blocked_executions
              WHERE concurrency_key = $1
              ORDER BY priority ASC, created_at ASC
              LIMIT 1
              FOR UPDATE SKIP LOCKED
            )
            RETURNING queue_name, payload
          SQL

          row = result.first
          return nil unless row

          { queue_name: row["queue_name"], payload: JSON.parse(row["payload"]) }
        end

        # Delete blocked executions that have expired.
        # Returns the count of deleted rows.
        def expire_stale
          result = execute(<<~SQL, "Pgbus Blocked Expire", [Time.now.utc])
            DELETE FROM pgbus_blocked_executions
            WHERE expires_at < $1
            RETURNING id
          SQL

          result.to_a.size
        end

        # Count blocked executions for a given key. Useful for testing/monitoring.
        def count_for(concurrency_key)
          result = execute(<<~SQL, "Pgbus Blocked Count", [concurrency_key])
            SELECT COUNT(*) AS cnt FROM pgbus_blocked_executions WHERE concurrency_key = $1
          SQL

          result.first&.fetch("cnt", 0).to_i
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
