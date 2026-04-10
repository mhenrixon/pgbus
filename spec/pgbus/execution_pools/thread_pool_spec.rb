# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ExecutionPools::ThreadPool do
  subject(:pool) { described_class.new(capacity: capacity) }

  let(:capacity) { 3 }

  after { pool.kill }

  describe "#initialize" do
    it "creates a pool with given capacity" do
      expect(pool.capacity).to eq(3)
    end

    it "starts fully available" do
      expect(pool.available_capacity).to eq(3)
    end
  end

  describe "#post" do
    it "fires on_state_change when work completes" do
      callback_called = Concurrent::Event.new
      pool_with_cb = described_class.new(
        capacity: 3,
        on_state_change: -> { callback_called.set }
      )

      pool_with_cb.post { nil }
      expect(callback_called.wait(2)).to be true
    ensure
      pool_with_cb&.kill
    end

    it "executes the submitted block" do
      result = Concurrent::IVar.new
      pool.post { result.set(42) }
      expect(result.value(2)).to eq(42)
    end

    it "executes multiple blocks concurrently" do
      results = Concurrent::Array.new
      latch = Concurrent::CountDownLatch.new(3)

      3.times do |i|
        pool.post do
          results << i
          latch.count_down
        end
      end

      expect(latch.wait(5)).to be true
      expect(results.size).to eq(3)
    end
  end

  describe "#available_capacity" do
    it "decreases when work is posted" do
      barrier = Concurrent::Event.new

      pool.post { barrier.wait(5) }
      sleep 0.05 # let thread start

      expect(pool.available_capacity).to be < capacity
      barrier.set
    end

    it "increases when work completes" do
      done = Concurrent::Event.new
      pool.post { done.set }
      done.wait(2)
      sleep 0.05 # let pool reclaim thread

      expect(pool.available_capacity).to eq(capacity)
    end
  end

  describe "#idle?" do
    it "is true when no work is queued" do
      expect(pool).to be_idle
    end

    it "is false when at capacity" do
      barrier = Concurrent::Event.new

      capacity.times { pool.post { barrier.wait(5) } }
      sleep 0.05

      expect(pool).not_to be_idle
      barrier.set
    end
  end

  describe "#shutdown" do
    it "finishes in-progress work" do
      result = Concurrent::IVar.new
      pool.post do
        sleep 0.05
        result.set(:done)
      end
      pool.shutdown

      expect(result.value(2)).to eq(:done)
    end
  end

  describe "#wait_for_termination" do
    it "blocks until all work completes" do
      result = Concurrent::IVar.new
      pool.post do
        sleep 0.05
        result.set(:done)
      end
      pool.shutdown
      pool.wait_for_termination(5)

      expect(result.value(0)).to eq(:done)
    end
  end

  describe "#kill" do
    it "abandons queued work" do
      pool.kill
      expect { pool.post { nil } }.to raise_error(Concurrent::RejectedExecutionError)
    end
  end

  describe "#metadata" do
    it "returns pool information" do
      meta = pool.metadata
      expect(meta[:mode]).to eq(:threads)
      expect(meta[:capacity]).to eq(3)
      expect(meta[:busy]).to eq(0)
    end
  end
end
