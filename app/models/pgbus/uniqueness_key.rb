# frozen_string_literal: true

module Pgbus
  class UniquenessKey < BusRecord
    self.table_name = "pgbus_uniqueness_keys"
    self.primary_key = "lock_key"

    # Atomically acquire a uniqueness lock within a PGMQ transaction.
    # Uses pg_advisory_xact_lock to serialize concurrent attempts for the
    # same key, then checks + inserts in the same transaction.
    #
    # Returns true if acquired (row inserted), false if already locked.
    def self.acquire!(lock_key, queue_name:, msg_id:) # rubocop:disable Naming/PredicateMethod
      connection.exec_query(
        "INSERT INTO #{table_name} (lock_key, queue_name, msg_id) " \
        "VALUES ($1, $2, $3) ON CONFLICT (lock_key) DO NOTHING RETURNING lock_key",
        "UniquenessKey Acquire", [lock_key, queue_name, msg_id]
      ).rows.any?
    end

    # Release a uniqueness lock after job completion or DLQ.
    def self.release!(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1",
        "UniquenessKey Release", [lock_key]
      )
    end

    # Check if a key is currently locked.
    def self.locked?(lock_key)
      result = connection.select_value(
        "SELECT 1 FROM #{table_name} WHERE lock_key = $1 LIMIT 1",
        "UniquenessKey Check", [lock_key]
      )
      !result.nil?
    end
  end
end
