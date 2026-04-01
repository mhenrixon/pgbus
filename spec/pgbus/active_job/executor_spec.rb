# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

RSpec.describe Pgbus::ActiveJob::Executor do
  subject(:executor) { described_class.new(client: mock_client, config: config) }

  let(:mock_client) { build_mock_client }
  let(:config) { Pgbus.configuration }
  let(:queue_name) { "default" }
  let(:job_id) { SecureRandom.uuid }
  let(:job_payload) do
    { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => queue_name, "arguments" => [] }
  end
  let(:message_json) { JSON.generate(job_payload) }
  let(:job_double) { build_job_double(job_class: "TestJob", queue_name: queue_name, job_id: job_id) }

  before do
    allow(ActiveSupport::Notifications).to receive(:instrument).and_call_original
    allow(ActiveJob::Base).to receive(:deserialize).with(job_payload).and_return(job_double)
    # By default, no concurrency key in payloads
    allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
    # Stub stat recording
    stub_const("Pgbus::JobStat", Class.new) unless defined?(Pgbus::JobStat)
    allow(Pgbus::JobStat).to receive(:record!)
  end

  describe "#execute" do
    context "when job succeeds" do
      let(:message) { build_message_double(msg_id: 5, message: message_json, read_ct: 1) }

      it "deserializes, performs, archives, instruments, and returns :success" do
        result = executor.execute(message, queue_name)

        expect(ActiveJob::Base).to have_received(:deserialize).with(job_payload)
        expect(job_double).to have_received(:perform_now)
        expect(mock_client).to have_received(:archive_message).with(queue_name, 5)
        expect(ActiveSupport::Notifications).to have_received(:instrument).with("pgbus.job_completed", queue: queue_name,
                                                                                                       job_class: "TestJob")
        expect(result).to eq(:success)
      end
    end

    context "when read_ct exceeds max_retries (DLQ routing)" do
      let(:message) { build_message_double(msg_id: 7, message: message_json, read_ct: config.max_retries + 1) }

      it "moves message to dead letter queue and returns :dead_lettered" do
        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:move_to_dead_letter).with(queue_name, message)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
        expect(result).to eq(:dead_lettered)
      end
    end

    context "when job.perform_now raises" do
      let(:message) { build_message_double(msg_id: 3, message: message_json, read_ct: 1) }
      let(:error) { StandardError.new("boom") }

      before do
        allow(job_double).to receive(:perform_now).and_raise(error)
      end

      it "logs the error, instruments failure, and returns :failed" do
        result = executor.execute(message, queue_name)

        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.job_failed",
          hash_including(queue: queue_name, job_class: "TestJob", error: "StandardError")
        )
        expect(result).to eq(:failed)
      end
    end

    context "when message payload is nil (JSON parse fails)" do
      let(:message) { build_message_double(msg_id: 9, message: nil, read_ct: 1) }

      it "returns :failed and safely handles nil payload via &.dig" do
        result = executor.execute(message, queue_name)

        expect(ActiveJob::Base).not_to have_received(:deserialize)
        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.job_failed",
          hash_including(queue: queue_name, job_class: nil)
        )
        expect(result).to eq(:failed)
      end
    end

    context "when instrumentation subscriber raises" do
      let(:message) { build_message_double(msg_id: 11, message: message_json, read_ct: 1) }

      before do
        allow(ActiveSupport::Notifications).to receive(:instrument)
          .with("pgbus.job_completed", anything)
          .and_raise(StandardError, "subscriber blew up")
      end

      it "rescues the subscriber error and still returns :success" do
        result = executor.execute(message, queue_name)

        expect(mock_client).to have_received(:archive_message).with(queue_name, 11)
        expect(result).to eq(:success)
      end
    end

    context "with batch_id in payload" do
      let(:batch_payload) { job_payload.merge("pgbus_batch_id" => "batch-abc") }
      let(:message_json) { JSON.generate(batch_payload) }
      let(:message) { build_message_double(msg_id: 30, message: message_json, read_ct: 1) }

      before do
        allow(ActiveJob::Base).to receive(:deserialize).with(batch_payload).and_return(job_double)
        allow(Pgbus::Batch).to receive(:job_completed)
        allow(Pgbus::Batch).to receive(:job_discarded)
      end

      it "signals batch completed on success" do
        executor.execute(message, queue_name)
        expect(Pgbus::Batch).to have_received(:job_completed).with("batch-abc")
      end

      it "signals batch discarded on dead letter" do
        dlq_message = build_message_double(msg_id: 31, message: message_json, read_ct: config.max_retries + 1)
        executor.execute(dlq_message, queue_name)
        expect(Pgbus::Batch).to have_received(:job_discarded).with("batch-abc")
      end

      it "does not signal batch on transient failure" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")
        executor.execute(message, queue_name)
        expect(Pgbus::Batch).not_to have_received(:job_completed)
        expect(Pgbus::Batch).not_to have_received(:job_discarded)
      end
    end

    context "with concurrency key in payload" do
      let(:concurrency_payload) { job_payload.merge("pgbus_concurrency_key" => "TestJob-42") }
      let(:message_json) { JSON.generate(concurrency_payload) }
      let(:message) { build_message_double(msg_id: 20, message: message_json, read_ct: 1) }

      before do
        allow(Pgbus::Concurrency).to receive(:extract_key).and_call_original
        allow(ActiveJob::Base).to receive(:deserialize).with(concurrency_payload).and_return(job_double)
        allow(Pgbus::Concurrency::Semaphore).to receive(:release)
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(false)
      end

      it "releases semaphore when no blocked jobs to promote" do
        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
        expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("TestJob-42")
      end

      it "skips semaphore release when promote_next succeeds (atomic handoff)" do
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(true)

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
      end

      it "releases semaphore on dead letter when no blocked jobs" do
        dlq_message = build_message_double(msg_id: 21, message: message_json, read_ct: config.max_retries + 1)

        executor.execute(dlq_message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("TestJob-42")
      end

      it "does not signal concurrency on transient failure" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "transient")

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
        expect(Pgbus::Concurrency::BlockedExecution).not_to have_received(:promote_next)
      end

      it "does not signal concurrency if archive_message fails (message will be retried)" do
        allow(mock_client).to receive(:archive_message).and_raise(StandardError, "DB gone")

        executor.execute(message, queue_name)

        expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
        expect(Pgbus::Concurrency::BlockedExecution).not_to have_received(:promote_next)
      end
    end

    context "with job stat recording" do
      let(:message) { build_message_double(msg_id: 1, message: message_json, read_ct: 1) }

      it "records a success stat after successful execution" do
        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "success",
          duration_ms: an_instance_of(Integer)
        )
      end

      it "records a failed stat when job raises" do
        allow(job_double).to receive(:perform_now).and_raise(StandardError, "boom")

        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "failed",
          duration_ms: an_instance_of(Integer)
        )
      end

      it "records a dead_lettered stat for DLQ routing" do
        dlq_message = build_message_double(msg_id: 99, message: message_json, read_ct: config.max_retries + 1)

        executor.execute(dlq_message, queue_name)

        expect(Pgbus::JobStat).to have_received(:record!).with(
          job_class: "TestJob",
          queue_name: queue_name,
          status: "dead_lettered",
          duration_ms: an_instance_of(Integer)
        )
      end

      it "does not record stats when stats_enabled is false" do
        config.stats_enabled = false

        executor.execute(message, queue_name)

        expect(Pgbus::JobStat).not_to have_received(:record!)
      ensure
        config.stats_enabled = true
      end
    end
  end
end
