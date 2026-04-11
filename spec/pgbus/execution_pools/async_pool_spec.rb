# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ExecutionPools::AsyncPool do
  subject(:pool) { described_class.new(capacity: capacity) }

  let(:capacity) { 3 }

  after do
    pool.shutdown
    pool.wait_for_termination(5)
  end

  describe "#initialize" do
    it "creates a pool with given capacity" do
      expect(pool.capacity).to eq(3)
    end

    it "starts fully available" do
      expect(pool.available_capacity).to eq(3)
    end

    it "boots the reactor thread synchronously" do
      # If we get here without hanging, boot sync worked.
      # Verify the pool is functional by posting and completing a task.
      result = Concurrent::IVar.new
      pool.post { result.set(:booted) }
      expect(result.value(5)).to eq(:booted)
    end
  end

  describe "#post" do
    it "executes the submitted block as a fiber" do
      result = Concurrent::IVar.new
      pool.post { result.set(42) }
      expect(result.value(5)).to eq(42)
    end

    it "executes multiple blocks concurrently" do
      results = Concurrent::Array.new
      done = Concurrent::CountDownLatch.new(3)

      3.times do |i|
        pool.post do
          results << i
          done.count_down
        end
      end

      expect(done.wait(5)).to be true
      expect(results.size).to eq(3)
    end

    it "raises when pool is shutting down" do
      pool.shutdown
      sleep 0.05
      expect { pool.post { nil } }.to raise_error(RuntimeError, /shutting down/)
    end

    it "raises when pool is at capacity" do
      barrier = Concurrent::Event.new

      capacity.times { pool.post { barrier.wait(5) } }
      sleep 0.05

      expect { pool.post { nil } }.to raise_error(RuntimeError, /at capacity/)
      barrier.set
    end
  end

  describe "#available_capacity" do
    it "decreases when work is posted" do
      barrier = Concurrent::Event.new

      pool.post { barrier.wait(5) }
      sleep 0.05

      expect(pool.available_capacity).to be < capacity
      barrier.set
    end

    it "increases when work completes" do
      done = Concurrent::Event.new
      pool.post { done.set }
      done.wait(5)
      sleep 0.05

      expect(pool.available_capacity).to eq(capacity)
    end
  end

  describe "#idle?" do
    it "is true when no work is queued" do
      expect(pool).to be_idle
    end
  end

  describe "#on_state_change callback" do
    it "fires when a fiber completes" do
      callback_called = Concurrent::Event.new
      pool_with_cb = described_class.new(
        capacity: 3,
        on_state_change: -> { callback_called.set }
      )

      pool_with_cb.post { nil }
      expect(callback_called.wait(5)).to be true
    ensure
      pool_with_cb&.shutdown
      pool_with_cb&.wait_for_termination(2)
    end
  end

  describe "#shutdown" do
    it "waits for in-flight fibers to complete" do
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

  describe "#metadata" do
    it "returns pool information" do
      meta = pool.metadata
      expect(meta[:mode]).to eq(:async)
      expect(meta[:capacity]).to eq(3)
      expect(meta[:busy]).to eq(0)
    end
  end

  describe "error handling" do
    it "does not crash the reactor when a fiber raises" do
      pool.post { raise "boom" }
      sleep 0.1

      # Pool should still accept new work
      result = Concurrent::IVar.new
      pool.post { result.set(:ok) }
      expect(result.value(5)).to eq(:ok)
    end

    # Regression: issue #126. The async gem raises Async::Stop and
    # Async::Cancel, both of which inherit from Exception (NOT StandardError).
    # A `rescue StandardError` misses them entirely and they propagate past
    # perform's ensure-less code path, leaving capacity unrestored and zero
    # telemetry. The pool must catch any Exception raised from user blocks
    # so capacity recovers and the error is logged.
    it "catches non-StandardError exceptions from fiber blocks and restores capacity" do
      fake_stop_class = Class.new(Exception) # rubocop:disable Lint/InheritException
      stub_const("FakeAsyncStop", fake_stop_class)

      allow(Pgbus.logger).to receive(:error)

      pool.post { raise fake_stop_class, "task cancelled" }
      sleep 0.1

      expect(pool.available_capacity).to eq(capacity)
      expect(Pgbus.logger).to have_received(:error).with(no_args) do |&block|
        expect(block.call).to include("FakeAsyncStop", "task cancelled")
      end

      # Pool still accepts new work
      result = Concurrent::IVar.new
      pool.post { result.set(:ok) }
      expect(result.value(5)).to eq(:ok)
    end
  end
end
