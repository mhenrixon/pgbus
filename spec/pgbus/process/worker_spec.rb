# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Worker do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:executor) { instance_double(Pgbus::ActiveJob::Executor) }
  let(:pool) { instance_double(Concurrent::FixedThreadPool, max_length: 5, queue_length: 0, shutdown: true, kill: true) }
  let(:worker) { described_class.new(queues: %w[default], threads: 5) }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
    allow(Concurrent::FixedThreadPool).to receive(:new).and_return(pool)
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
        worker.stats[:jobs_processed] = 100
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        worker.stats[:jobs_processed] = 50
        expect(worker.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { worker.config.max_worker_lifetime = 60 }

      it "returns true when lifetime is exceeded" do
        worker.stats[:started_at] = Time.now - 120
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

  describe "#process_message (private)" do
    let(:message) { build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}') }

    it "executes the message and increments jobs_processed" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(executor).to have_received(:execute).with(message, "default")
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
  end
end
