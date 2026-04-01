# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job uniqueness (integration)", :integration do
  before do
    Pgbus::JobLock.delete_all
  end

  describe ":until_executed strategy" do
    it "prevents duplicate enqueue when lock is held" do
      acquired = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", ttl: 300)
      expect(acquired).to be true

      # Second acquire should fail
      duplicate = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j2", ttl: 300)
      expect(duplicate).to be false
    end

    it "allows enqueue after lock is released" do
      Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", ttl: 300)
      Pgbus::JobLock.release!("unique-order-42")

      acquired = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j3", ttl: 300)
      expect(acquired).to be true
    end

    it "allows enqueue after lock expires" do
      Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j1", ttl: -1)

      # Expired locks are cleaned up
      Pgbus::JobLock.cleanup_expired!

      acquired = Pgbus::JobLock.acquire!("unique-order-42", job_class: "ImportJob", job_id: "j4", ttl: 300)
      expect(acquired).to be true
    end

    it "reports locked? correctly" do
      expect(Pgbus::JobLock.locked?("unique-order-99")).to be false

      Pgbus::JobLock.acquire!("unique-order-99", job_class: "Test", ttl: 300)
      expect(Pgbus::JobLock.locked?("unique-order-99")).to be true

      Pgbus::JobLock.release!("unique-order-99")
      expect(Pgbus::JobLock.locked?("unique-order-99")).to be false
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
            ttl: 300
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

  describe "lock cleanup" do
    it "cleans up expired locks" do
      # Create some expired locks
      3.times do |i|
        Pgbus::JobLock.acquire!("expired-#{i}", job_class: "OldJob", ttl: -1)
      end
      # Create a valid lock
      Pgbus::JobLock.acquire!("valid-lock", job_class: "NewJob", ttl: 300)

      deleted = Pgbus::JobLock.cleanup_expired!
      expect(deleted).to eq(3)

      # Valid lock should still exist
      expect(Pgbus::JobLock.locked?("valid-lock")).to be true
    end
  end
end
