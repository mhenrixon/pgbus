# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job uniqueness (integration)", :integration do
  before do
    Pgbus::JobLock.delete_all
    Pgbus::ProcessEntry.delete_all
  end

  describe ":until_executed strategy" do
    it "prevents duplicate enqueue when lock is held" do
      acquired = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", ttl: 86_400)
      expect(acquired).to be true

      duplicate = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j2", ttl: 86_400)
      expect(duplicate).to be false
    end

    it "allows enqueue after lock is released" do
      Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", ttl: 86_400)
      Pgbus::JobLock.release!("unique-order-42")

      acquired = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j3", ttl: 86_400)
      expect(acquired).to be true
    end

    it "transitions from queued to executing on claim" do
      Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", state: "queued", ttl: 86_400)

      Pgbus::JobLock.claim_for_execution!("unique-order-42", owner_pid: 12_345, owner_hostname: "test-host", ttl: 86_400)

      lock = Pgbus::JobLock.find_by(lock_key: "unique-order-42")
      expect(lock.state).to eq("executing")
      expect(lock.owner_pid).to eq(12_345)
      expect(lock.owner_hostname).to eq("test-host")
    end
  end

  describe "reaper" do
    it "releases locks whose owner worker is no longer alive" do
      # Create a lock owned by PID 99999 (not in pgbus_processes)
      Pgbus::JobLock.acquire!(
        "orphaned-lock", job_class: "CrashedJob", job_id: "j1",
                         state: "executing", owner_pid: 99_999, owner_hostname: "dead-host", ttl: 86_400
      )

      reaped = Pgbus::JobLock.reap_orphaned!
      expect(reaped).to eq(1)
      expect(Pgbus::JobLock.locked?("orphaned-lock")).to be false
    end

    it "keeps locks whose owner worker is still alive" do
      # Register a healthy worker process
      Pgbus::ProcessEntry.create!(
        kind: "worker", pid: 12_345, hostname: "test-host",
        last_heartbeat_at: Time.current
      )

      # Create a lock owned by that worker (same pid AND hostname)
      Pgbus::JobLock.acquire!(
        "active-lock", job_class: "RunningJob", job_id: "j1",
                       state: "executing", owner_pid: 12_345, owner_hostname: "test-host", ttl: 86_400
      )

      reaped = Pgbus::JobLock.reap_orphaned!
      expect(reaped).to eq(0)
      expect(Pgbus::JobLock.locked?("active-lock")).to be true
    end

    it "releases locks whose owner worker has stale heartbeat" do
      # Register a stale worker process (heartbeat too old)
      Pgbus::ProcessEntry.create!(
        kind: "worker", pid: 12_345, hostname: "stale-host",
        last_heartbeat_at: Time.current - 600 # 10 minutes ago
      )

      Pgbus::JobLock.acquire!(
        "stale-lock", job_class: "StaleJob", job_id: "j1",
                      state: "executing", owner_pid: 12_345, owner_hostname: "stale-host", ttl: 86_400
      )

      reaped = Pgbus::JobLock.reap_orphaned!
      expect(reaped).to eq(1)
      expect(Pgbus::JobLock.locked?("stale-lock")).to be false
    end

    it "does not reap queued locks (they have no owner)" do
      Pgbus::JobLock.acquire!(
        "queued-lock", job_class: "WaitingJob", job_id: "j1",
                       state: "queued", ttl: 86_400
      )

      reaped = Pgbus::JobLock.reap_orphaned!
      expect(reaped).to eq(0)
      expect(Pgbus::JobLock.locked?("queued-lock")).to be true
    end
  end

  describe "last-resort TTL" do
    it "cleans up locks whose TTL has expired" do
      Pgbus::JobLock.acquire!("expired-lock", job_class: "OldJob", ttl: -1)

      deleted = Pgbus::JobLock.cleanup_expired!
      expect(deleted).to eq(1)
    end

    it "does not clean up locks with valid TTL" do
      Pgbus::JobLock.acquire!("valid-lock", job_class: "NewJob", ttl: 86_400)

      deleted = Pgbus::JobLock.cleanup_expired!
      expect(deleted).to eq(0)
      expect(Pgbus::JobLock.locked?("valid-lock")).to be true
    end
  end

  describe "concurrent lock acquisition" do
    it "only one thread wins the lock" do
      results = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(5)

      threads = 5.times.map do |i|
        Thread.new do
          barrier.wait
          acquired = Pgbus::JobLock.acquire!(
            "race-key",
            job_class: "RaceJob",
            job_id: "thread-#{i}",
            ttl: 86_400
          )
          results << acquired
        end
      end

      threads.each(&:join)

      winners = results.count(true)
      losers = results.count(false)

      expect(winners).to eq(1)
      expect(losers).to eq(4)
    end
  end
end
