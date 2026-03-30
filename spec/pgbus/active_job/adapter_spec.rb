# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ActiveJob::Adapter do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:job_id) { SecureRandom.uuid }
  let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
  let(:serialized_json) { "{\"job_class\":\"TestJob\",\"job_id\":\"#{job_id}\",\"queue_name\":\"default\",\"arguments\":[]}" }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::Serializer).to receive(:serialize_job).and_return(serialized_json)
  end

  describe "#enqueue" do
    it "serializes the job, sends a message, sets provider_job_id, and returns the job" do
      allow(mock_client).to receive(:send_message).and_return(42)

      result = adapter.enqueue(job)

      expect(Pgbus::Serializer).to have_received(:serialize_job).with(job)
      expect(mock_client).to have_received(:send_message).with("default", JSON.parse(serialized_json))
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

        expect(mock_client).to have_received(:send_message).with("default", anything)
      end
    end
  end

  describe "#enqueue_at" do
    it "calculates delay and sends message with delay parameter" do
      future_time = Time.now.to_f + 60
      allow(mock_client).to receive(:send_message).and_return(99)

      result = adapter.enqueue_at(job, future_time)

      expect(mock_client).to have_received(:send_message).with("default", JSON.parse(serialized_json), delay: a_value_between(59, 61))
      expect(job).to have_received(:provider_job_id=).with(99)
      expect(result).to eq(job)
    end

    context "when timestamp is in the past" do
      it "uses delay 0" do
        past_time = Time.now.to_f - 100
        allow(mock_client).to receive(:send_message).and_return(7)

        adapter.enqueue_at(job, past_time)

        expect(mock_client).to have_received(:send_message).with("default", JSON.parse(serialized_json), delay: 0)
      end
    end
  end

  describe "#enqueue_all" do
    let(:second_job_id) { SecureRandom.uuid }
    let(:job2) { build_job_double(job_class: "OtherJob", queue_name: "default", job_id: second_job_id) }
    let(:second_serialized_json) do
      "{\"job_class\":\"OtherJob\",\"job_id\":\"#{second_job_id}\",\"queue_name\":\"default\",\"arguments\":[]}"
    end

    before do
      allow(job).to receive(:scheduled_at).and_return(nil)
      allow(job2).to receive(:scheduled_at).and_return(nil)
      allow(Pgbus::Serializer).to receive(:serialize_job).with(job).and_return(serialized_json)
      allow(Pgbus::Serializer).to receive(:serialize_job).with(job2).and_return(second_serialized_json)
    end

    it "batches immediate jobs via send_batch" do
      allow(mock_client).to receive(:send_batch).and_return([1, 2])

      result = adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch).with("default", [JSON.parse(serialized_json), JSON.parse(second_serialized_json)])
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

      expect(mock_client).to have_received(:send_message).with("default", JSON.parse(serialized_json), delay: a_value > 0)
      expect(mock_client).to have_received(:send_batch).with("default", [JSON.parse(second_serialized_json)])
    end

    context "when batch response size mismatches" do
      it "raises an error" do
        allow(mock_client).to receive(:send_batch).and_return([1])

        expect { adapter.enqueue_all([job, job2]) }.to raise_error(RuntimeError, /batch enqueue failed/)
      end
    end
  end
end
