# frozen_string_literal: true

module Pgbus
  class JobLock < Pgbus::ApplicationRecord
    self.table_name = "pgbus_job_locks"

    scope :expired, ->(now = Time.current) { where("expires_at < ?", now) }

    # Atomically try to acquire a lock. Cleans up expired locks in the same
    # query path so stale locks don't block new acquisitions until the next
    # dispatcher cleanup cycle.
    #
    # Returns true if acquired, false if already locked by an active (non-expired) lock.
    def self.acquire!(lock_key, job_class:, ttl:, job_id: nil)
      # First, remove any expired lock for this key (crash recovery at acquire time)
      where(lock_key: lock_key).where("expires_at < ?", Time.current).delete_all

      result = insert(
        { lock_key: lock_key, job_class: job_class, job_id: job_id, expires_at: Time.current + ttl },
        unique_by: :lock_key
      )
      result.rows.any?
    rescue ActiveRecord::RecordNotUnique
      false
    end

    # Release a lock by key.
    def self.release!(lock_key)
      where(lock_key: lock_key).delete_all
    end

    # Check if a lock is currently held (and not expired).
    def self.locked?(lock_key)
      where(lock_key: lock_key).where("expires_at >= ?", Time.current).exists?
    end

    # Delete all expired locks. Called by the dispatcher.
    def self.cleanup_expired!
      expired.delete_all
    end
  end
end
