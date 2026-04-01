# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Concurrency semaphores (integration)", :integration do
  before do
    Pgbus::Semaphore.delete_all
  end

  describe "atomic acquire" do
    it "acquires a semaphore slot" do
      result = Pgbus::Concurrency::Semaphore.acquire("test-key", 2, 300)
      expect(result).to eq(:acquired)
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(1)
    end

    it "allows multiple slots up to the limit" do
      Pgbus::Concurrency::Semaphore.acquire("test-key", 3, 300)
      Pgbus::Concurrency::Semaphore.acquire("test-key", 3, 300)
      result = Pgbus::Concurrency::Semaphore.acquire("test-key", 3, 300)

      expect(result).to eq(:acquired)
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(3)
    end

    it "blocks when limit is reached" do
      3.times { Pgbus::Concurrency::Semaphore.acquire("test-key", 3, 300) }

      result = Pgbus::Concurrency::Semaphore.acquire("test-key", 3, 300)
      expect(result).to eq(:blocked)
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(3)
    end
  end

  describe "release" do
    it "decrements the semaphore value" do
      Pgbus::Concurrency::Semaphore.acquire("test-key", 2, 300)
      Pgbus::Concurrency::Semaphore.acquire("test-key", 2, 300)
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(2)

      Pgbus::Concurrency::Semaphore.release("test-key")
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(1)
    end

    it "never goes below zero" do
      Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 300)
      Pgbus::Concurrency::Semaphore.release("test-key")
      Pgbus::Concurrency::Semaphore.release("test-key")
      expect(Pgbus::Concurrency::Semaphore.current_value("test-key")).to eq(0)
    end

    it "opens a slot for the next acquire" do
      Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 300)
      expect(Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 300)).to eq(:blocked)

      Pgbus::Concurrency::Semaphore.release("test-key")
      expect(Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 300)).to eq(:acquired)
    end
  end

  describe "concurrent acquire (race condition)" do
    it "never exceeds the limit under contention" do
      limit = 3
      results = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(10)

      threads = 10.times.map do
        Thread.new do
          barrier.wait
          result = Pgbus::Concurrency::Semaphore.acquire("race-key", limit, 300)
          results << result
        end
      end

      threads.each(&:join)

      acquired = results.count(:acquired)
      blocked = results.count(:blocked)

      expect(acquired).to eq(limit)
      expect(blocked).to eq(7)
      expect(Pgbus::Concurrency::Semaphore.current_value("race-key")).to eq(limit)
    end
  end

  describe "expiry" do
    it "cleans up expired semaphores" do
      Pgbus::Concurrency::Semaphore.acquire("expired-key", 1, -1)
      Pgbus::Concurrency::Semaphore.acquire("active-key", 1, 300)

      expired = Pgbus::Concurrency::Semaphore.expire_stale
      expect(expired.size).to eq(1)
      expect(expired.first["key"]).to eq("expired-key")

      # Active key should still exist
      expect(Pgbus::Concurrency::Semaphore.current_value("active-key")).to eq(1)
      expect(Pgbus::Concurrency::Semaphore.current_value("expired-key")).to be_nil
    end
  end
end
