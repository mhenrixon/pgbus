# frozen_string_literal: true

module Pgbus
  class JobLock < BusRecord
    self.table_name = "pgbus_job_locks"

    # States:
    #   queued    — lock held from enqueue time (:until_executed), no worker yet
    #   executing — lock held by an active worker process
    STATES = %w[queued executing].freeze

    scope :executing, -> { where(state: "executing") }
    scope :queued_locks, -> { where(state: "queued") }
    scope :expired, ->(now = Time.current) { where("expires_at < ?", now) }

    # Atomically try to acquire a lock.
    # Cleans up expired locks for this key first (crash recovery at acquire time).
    # Returns true if acquired, false if already locked.
    #
    # Uses raw SQL on the hot path to minimize ActiveRecord allocations
    # (~29 objects vs ~304 per acquire+release cycle with AR query builder).
    def self.acquire!(lock_key, job_class:, ttl:, job_id: nil, state: "queued", owner_pid: nil, owner_hostname: nil) # rubocop:disable Naming/PredicateMethod
      expires_at = Time.current + ttl

      # Remove any expired lock for this key inline (last-resort TTL recovery)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1 AND expires_at < $2",
        "JobLock Expire", [lock_key, Time.current]
      )

      result = connection.exec_query(
        "INSERT INTO #{table_name} (lock_key, job_class, job_id, state, owner_pid, owner_hostname, expires_at) " \
        "VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (lock_key) DO NOTHING RETURNING id",
        "JobLock Acquire", [lock_key, job_class, job_id, state, owner_pid, owner_hostname, expires_at]
      )
      result.rows.any?
    end

    # Transition a queued lock to executing state and claim ownership.
    # Called when a worker starts executing a job that was locked at enqueue time.
    def self.claim_for_execution!(lock_key, owner_pid:, owner_hostname:, ttl:)
      connection.exec_update(
        "UPDATE #{table_name} SET state = $1, owner_pid = $2, owner_hostname = $3, expires_at = $4 " \
        "WHERE lock_key = $5",
        "JobLock Claim", ["executing", owner_pid, owner_hostname, Time.current + ttl, lock_key]
      )
    end

    # Release a lock by key.
    def self.release!(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1",
        "JobLock Release", [lock_key]
      )
    end

    # Check if a lock is currently held (regardless of expiry — reaper handles orphans).
    def self.locked?(lock_key)
      result = connection.select_value(
        "SELECT 1 FROM #{table_name} WHERE lock_key = $1 LIMIT 1", "JobLock Check", [lock_key]
      )
      !result.nil?
    end

    # Reap orphaned locks: locks in 'executing' state whose owner_pid
    # has no healthy entry in pgbus_processes.
    # Returns the number of orphaned locks released.
    # Reap orphaned locks by matching (pid, hostname) against live process entries.
    # A lock is orphaned if no healthy process exists with the same pid AND hostname.
    def self.reap_orphaned!
      alive_workers = ProcessEntry
                      .where("last_heartbeat_at >= ?", Time.current - Process::Heartbeat::ALIVE_THRESHOLD)
                      .pluck(:pid, :hostname)

      orphaned = executing.select do |lock|
        alive_workers.none? { |pid, hostname| pid == lock.owner_pid && hostname == lock.owner_hostname }
      end

      return 0 if orphaned.empty?

      where(id: orphaned.map(&:id)).delete_all
    end

    # Last-resort cleanup: delete locks whose expires_at has passed.
    # This only fires when the reaper itself can't run (e.g., entire supervisor dead).
    def self.cleanup_expired!
      expired.delete_all
    end
  end
end
