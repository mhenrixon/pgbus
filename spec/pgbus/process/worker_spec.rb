# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Worker do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:executor) { instance_double(Pgbus::ActiveJob::Executor) }
  let(:pool) { instance_double(Concurrent::FixedThreadPool, max_length: 5, queue_length: 0, shutdown: true, kill: true) }
  let(:circuit_breaker) { instance_double(Pgbus::CircuitBreaker, paused?: false, record_success: nil, record_failure: nil) }
  let(:worker) { described_class.new(queues: %w[default], threads: 5) }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
    allow(Concurrent::FixedThreadPool).to receive(:new).and_return(pool)
    allow(Pgbus::CircuitBreaker).to receive(:new).and_return(circuit_breaker)
  end

  describe "#initialize" do
    it "stores config, queues, and creates a thread pool" do
      expect(worker.queues).to eq(%w[default])
      expect(worker.threads).to eq(5)
      expect(worker.config).to be_a(Pgbus::Configuration)
      expect(Concurrent::FixedThreadPool).to have_received(:new).with(5)
    end

    it "initializes stats tracking" do
      expect(worker.stats[:jobs_processed]).to eq(0)
      expect(worker.stats[:jobs_failed]).to eq(0)
      expect(worker.stats[:in_flight]).to eq(0)
      expect(worker.stats[:started_at]).to be_a(Time)
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      worker.graceful_shutdown
      expect(worker.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down flag and kills the pool" do
      worker.immediate_shutdown
      expect(worker.instance_variable_get(:@shutting_down)).to be true
      expect(pool).to have_received(:kill)
    end
  end

  describe "#recycle_needed? (private)" do
    it "returns false when no limits are configured" do
      expect(worker.send(:recycle_needed?)).to be false
    end

    context "when max_jobs_per_worker is exceeded" do
      before { worker.config.max_jobs_per_worker = 100 }

      it "returns true when jobs_processed reaches the limit" do
        worker.instance_variable_get(:@jobs_processed).value = 100
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        worker.instance_variable_get(:@jobs_processed).value = 50
        expect(worker.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { worker.config.max_worker_lifetime = 60 }

      it "returns true when lifetime is exceeded" do
        worker.instance_variable_set(:@started_at, Time.now - 120)
        expect(worker.send(:recycle_needed?)).to be true
      end
    end
  end

  describe "#fetch_messages (private)" do
    context "with a single queue" do
      it "reads a batch from the single queue" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default priority], threads: 4) }

      it "reads proportionally from each queue" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:fetch_messages, 4)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 2)
        expect(mock_client).to have_received(:read_batch).with("priority", qty: 2)
      end
    end

    context "when an error occurs" do
      before { allow(mock_client).to receive(:read_batch).and_raise(StandardError, "connection lost") }

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end
    end
  end

  describe "prefetch flow control" do
    before { allow(worker).to receive(:interruptible_sleep) }

    context "when prefetch_limit is nil (default)" do
      before { worker.config.prefetch_limit = nil }

      it "does not cap fetch quantity" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "when prefetch_limit is configured" do
      before { worker.config.prefetch_limit = 3 }
      after { worker.config.prefetch_limit = nil }

      it "caps fetch to prefetch_limit when below idle threads" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 3)
      end

      it "sleeps when in_flight >= prefetch_limit" do
        worker.instance_variable_get(:@in_flight).value = 3
        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch)
        expect(worker).to have_received(:interruptible_sleep)
      end

      it "uses min of idle and available prefetch" do
        # 2 in flight, limit 3 => available = 1, idle = 5 => fetch 1
        worker.instance_variable_get(:@in_flight).value = 2
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 1)
      end
    end

    it "increments in_flight when messages are fetched" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:claim_and_execute)
      # After process_message completes (via yield), in_flight should be back to 0
      expect(worker.stats[:in_flight]).to eq(0)
    end

    it "decrements in_flight even when executor raises" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:claim_and_execute)
      expect(worker.stats[:in_flight]).to eq(0)
    end
  end

  describe "#process_message (private)" do
    let(:message) { build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}') }

    it "executes the message and increments jobs_processed" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(executor).to have_received(:execute).with(message, "default", source_queue: nil)
      expect(worker.stats[:jobs_processed]).to eq(1)
    end

    it "increments jobs_failed when executor returns :failed" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_processed]).to eq(1)
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "increments jobs_failed when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "signals circuit breaker success on successful execution" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_success).with("default")
    end

    it "signals circuit breaker failure on failed execution" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end

    it "signals circuit breaker failure when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end
  end

  describe "circuit breaker integration" do
    before { allow(worker).to receive(:interruptible_sleep) }

    it "skips paused queues" do
      allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
      allow(mock_client).to receive(:read_batch).and_return([])

      worker.send(:claim_and_execute)
      expect(mock_client).not_to have_received(:read_batch)
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default events], threads: 4) }

      it "only reads from non-paused queues" do
        allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
        allow(circuit_breaker).to receive(:paused?).with("events").and_return(false)
        allow(mock_client).to receive(:read_batch).and_return([])

        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch).with("default", anything)
        # With only one active queue, pool max_length (5) is the fetch qty
        expect(mock_client).to have_received(:read_batch).with("events", qty: 5)
      end
    end
  end
end
