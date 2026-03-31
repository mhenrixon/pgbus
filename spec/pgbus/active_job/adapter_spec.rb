# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ActiveJob::Adapter do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:job_id) { SecureRandom.uuid }
  let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
  let(:serialized_hash) { { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] } }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
  end

  describe "#enqueue" do
    it "serializes the job, sends a message, sets provider_job_id, and returns the job" do
      allow(mock_client).to receive(:send_message).and_return(42)

      result = adapter.enqueue(job)

      expect(Pgbus::Serializer).to have_received(:serialize_job_hash).with(job)
      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: 0)
      expect(job).to have_received(:provider_job_id=).with(42)
      expect(result).to eq(job)
    end

    context "when queue_name is nil" do
      let(:job) { build_job_double(job_class: "TestJob", queue_name: nil, job_id: job_id) }

      before do
        allow(job).to receive(:queue_name).and_return(nil)
      end

      it "falls back to config.default_queue" do
        allow(mock_client).to receive(:send_message).and_return(1)

        adapter.enqueue(job)

        expect(mock_client).to have_received(:send_message).with("default", anything, delay: 0)
      end
    end
  end

  describe "#enqueue_at" do
    it "calculates delay and sends message with delay parameter" do
      future_time = Time.now.to_f + 60
      allow(mock_client).to receive(:send_message).and_return(99)

      result = adapter.enqueue_at(job, future_time)

      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: a_value_between(59, 61))
      expect(job).to have_received(:provider_job_id=).with(99)
      expect(result).to eq(job)
    end

    context "when timestamp is in the past" do
      it "uses delay 0" do
        past_time = Time.now.to_f - 100
        allow(mock_client).to receive(:send_message).and_return(7)

        adapter.enqueue_at(job, past_time)

        expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: 0)
      end
    end
  end

  describe "#enqueue with concurrency" do
    let(:concurrency_config) do
      { limit: 1, duration: 900, on_conflict: :block, key: ->(*) { "TestJob-42" } }
    end
    let(:job_class_double) do
      double("JobClass", pgbus_concurrency: concurrency_config, name: "TestJob").tap do |klass|
        allow(klass).to receive(:respond_to?).and_return(false)
        allow(klass).to receive(:respond_to?).with(:pgbus_concurrency).and_return(true)
      end
    end
    let(:concurrency_payload) do
      serialized_hash.merge("pgbus_concurrency_key" => "TestJob-42")
    end

    before do
      allow(Pgbus::Concurrency).to receive_messages(inject_metadata: concurrency_payload, extract_key: "TestJob-42")
      allow(job).to receive(:class).and_return(job_class_double)
    end

    it "acquires semaphore and enqueues when under limit" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(mock_client).to receive(:send_message).and_return(42)

      adapter.enqueue(job)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire).with("TestJob-42", 1, 900)
      expect(mock_client).to have_received(:send_message).with("default", concurrency_payload, delay: 0)
      expect(job).to have_received(:provider_job_id=).with(42)
    end

    it "blocks when at concurrency limit with on_conflict: :block" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
      allow(job).to receive(:try).with(:priority).and_return(0)

      adapter.enqueue(job)

      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).with(
        concurrency_key: "TestJob-42",
        queue_name: "default",
        payload: concurrency_payload,
        priority: 0,
        duration: 900
      )
      expect(mock_client).not_to have_received(:send_message)
    end

    it "discards when at concurrency limit with on_conflict: :discard" do
      allow(job_class_double).to receive(:pgbus_concurrency).and_return(
        concurrency_config.merge(on_conflict: :discard)
      )
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
    end

    it "raises when at concurrency limit with on_conflict: :raise" do
      allow(job_class_double).to receive(:pgbus_concurrency).and_return(
        concurrency_config.merge(on_conflict: :raise)
      )
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

      expect { adapter.enqueue(job) }.to raise_error(Pgbus::ConcurrencyLimitExceeded, /TestJob-42/)
    end
  end

  describe "#enqueue_all" do
    let(:second_job_id) { SecureRandom.uuid }
    let(:job2) { build_job_double(job_class: "OtherJob", queue_name: "default", job_id: second_job_id) }
    let(:second_serialized_hash) do
      { "job_class" => "OtherJob", "job_id" => second_job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(job).to receive(:scheduled_at).and_return(nil)
      allow(job2).to receive(:scheduled_at).and_return(nil)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job).and_return(serialized_hash)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job2).and_return(second_serialized_hash)
    end

    it "batches immediate jobs via send_batch" do
      allow(mock_client).to receive(:send_batch).and_return([1, 2])

      result = adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch).with("default", [serialized_hash, second_serialized_hash])
      expect(job).to have_received(:provider_job_id=).with(1)
      expect(job2).to have_received(:provider_job_id=).with(2)
      expect(result).to eq(2)
    end

    it "schedules future jobs individually via enqueue_at" do
      future_time = Time.now + 120
      allow(job).to receive(:scheduled_at).and_return(future_time)
      allow(job2).to receive(:scheduled_at).and_return(nil)

      # job is scheduled in the future -> enqueue_at individually
      # job2 is immediate -> send_batch
      allow(mock_client).to receive_messages(send_message: 10, send_batch: [20])

      adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: a_value > 0)
      expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash])
    end

    context "when batch response size mismatches" do
      it "raises an error" do
        allow(mock_client).to receive(:send_batch).and_return([1])

        expect { adapter.enqueue_all([job, job2]) }.to raise_error(RuntimeError, /batch enqueue failed/)
      end
    end
  end
end
