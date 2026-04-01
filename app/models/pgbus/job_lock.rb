# frozen_string_literal: true

module Pgbus
  class JobLock < Pgbus::ApplicationRecord
    self.table_name = "pgbus_job_locks"

    scope :expired, ->(now = Time.current) { where("expires_at < ?", now) }

    # Atomically try to acquire a lock. Returns true if acquired, false if already locked.
    # Uses INSERT ... ON CONFLICT DO NOTHING for race-free acquisition.
    def self.acquire!(lock_key, job_class:, ttl:, job_id: nil)
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
