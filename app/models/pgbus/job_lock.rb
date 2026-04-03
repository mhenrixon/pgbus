# frozen_string_literal: true

module Pgbus
  class JobLock < Pgbus::BusRecord
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
    def self.acquire!(lock_key, job_class:, ttl:, job_id: nil, state: "queued", owner_pid: nil, owner_hostname: nil)
      # Remove any expired lock for this key inline (last-resort TTL recovery)
      where(lock_key: lock_key).where("expires_at < ?", Time.current).delete_all

      result = insert(
        {
          lock_key: lock_key, job_class: job_class, job_id: job_id,
          state: state, owner_pid: owner_pid, owner_hostname: owner_hostname,
          expires_at: Time.current + ttl
        },
        unique_by: :lock_key
      )
      result.rows.any?
    rescue ActiveRecord::RecordNotUnique
      false
    end

    # Transition a queued lock to executing state and claim ownership.
    # Called when a worker starts executing a job that was locked at enqueue time.
    def self.claim_for_execution!(lock_key, owner_pid:, owner_hostname:, ttl:)
      where(lock_key: lock_key).update_all(
        state: "executing",
        owner_pid: owner_pid,
        owner_hostname: owner_hostname,
        expires_at: Time.current + ttl
      )
    end

    # Release a lock by key.
    def self.release!(lock_key)
      where(lock_key: lock_key).delete_all
    end

    # Check if a lock is currently held (regardless of expiry — reaper handles orphans).
    def self.locked?(lock_key)
      where(lock_key: lock_key).exists?
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
