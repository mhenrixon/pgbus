# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Batch do
  let(:batch_record_double) { double("BatchRecord", id: 1, attributes: { "batch_id" => "abc" }) }

  before do
    allow(Pgbus::BatchRecord).to receive(:create!).and_return(batch_record_double)
    allow(Pgbus::BatchRecord).to receive_message_chain(:where, :update_all).and_return(1) # rubocop:disable RSpec/MessageChain
  end

  describe "#initialize" do
    it "generates a UUID batch_id" do
      batch = described_class.new
      expect(batch.batch_id).to match(/\A[0-9a-f-]{36}\z/)
    end

    it "stores callback classes and properties" do
      callback_class = Class.new
      batch = described_class.new(
        on_finish: callback_class,
        description: "test batch",
        properties: { user_id: 1 }
      )
      expect(batch.on_finish).to eq(callback_class)
      expect(batch.description).to eq("test batch")
      expect(batch.properties[:user_id]).to eq(1)
    end
  end

  describe "#enqueue" do
    it "creates a batch record in the database" do
      batch = described_class.new(description: "test")
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchRecord).to have_received(:create!).with(
        hash_including(batch_id: batch.batch_id, description: "test", status: "pending")
      )
    end

    it "updates total_jobs after counting" do
      batch = described_class.new
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchRecord).to have_received(:where).with(batch_id: batch.batch_id)
    end

    it "sets thread-local batch_id during block execution" do
      captured_batch_id = nil
      batch = described_class.new

      batch.enqueue do
        captured_batch_id = Thread.current[:pgbus_batch_id]
      end

      expect(captured_batch_id).to eq(batch.batch_id)
      expect(Thread.current[:pgbus_batch_id]).to be_nil
    end
  end

  describe ".job_completed" do
    it "increments completed_jobs counter" do
      row = {
        "status" => "processing",
        "total_jobs" => "3",
        "completed_jobs" => "1",
        "discarded_jobs" => "0"
      }
      allow(Pgbus::BatchRecord).to receive(:increment_counter!).and_return(row)

      described_class.job_completed("batch-123")

      expect(Pgbus::BatchRecord).to have_received(:increment_counter!).with("batch-123", "completed_jobs")
    end

    it "fires on_finish callback when batch finishes" do
      row = {
        "status" => "finished",
        "total_jobs" => "2",
        "completed_jobs" => "2",
        "discarded_jobs" => "0",
        "on_finish_class" => "BatchCallbackJob",
        "on_success_class" => nil,
        "on_discard_class" => nil,
        "properties" => '{"user_id":1}',
        "just_finished" => true
      }
      allow(Pgbus::BatchRecord).to receive(:increment_counter!).and_return(row)

      callback_job = class_double("BatchCallbackJob", perform_later: nil) # rubocop:disable RSpec/VerifiedDoubleReference
      stub_const("BatchCallbackJob", callback_job)

      described_class.job_completed("batch-123")

      expect(callback_job).to have_received(:perform_later).with({ "user_id" => 1 })
    end

    it "fires on_success callback when all jobs succeed" do
      row = {
        "status" => "finished",
        "total_jobs" => "1",
        "completed_jobs" => "1",
        "discarded_jobs" => "0",
        "on_finish_class" => nil,
        "on_success_class" => "SuccessJob",
        "on_discard_class" => nil,
        "properties" => "{}",
        "just_finished" => true
      }
      allow(Pgbus::BatchRecord).to receive(:increment_counter!).and_return(row)

      callback_job = class_double("SuccessJob", perform_later: nil) # rubocop:disable RSpec/VerifiedDoubleReference
      stub_const("SuccessJob", callback_job)

      described_class.job_completed("batch-123")

      expect(callback_job).to have_received(:perform_later)
    end

    it "fires on_discard callback when some jobs were discarded" do
      row = {
        "status" => "finished",
        "total_jobs" => "2",
        "completed_jobs" => "1",
        "discarded_jobs" => "1",
        "on_finish_class" => nil,
        "on_success_class" => nil,
        "on_discard_class" => "DiscardJob",
        "properties" => "{}",
        "just_finished" => true
      }
      allow(Pgbus::BatchRecord).to receive(:increment_counter!).and_return(row)

      callback_job = class_double("DiscardJob", perform_later: nil) # rubocop:disable RSpec/VerifiedDoubleReference
      stub_const("DiscardJob", callback_job)

      described_class.job_discarded("batch-123")

      expect(callback_job).to have_received(:perform_later)
    end

    it "returns nil when batch not found" do
      allow(Pgbus::BatchRecord).to receive(:increment_counter!).and_return(nil)

      expect(described_class.job_completed("nonexistent")).to be_nil
    end
  end

  describe ".find" do
    it "returns the batch record attributes" do
      record = double("BatchRecord", attributes: { "batch_id" => "abc", "status" => "processing" })
      allow(Pgbus::BatchRecord).to receive(:find_by).with(batch_id: "abc").and_return(record)

      expect(described_class.find("abc")).to eq({ "batch_id" => "abc", "status" => "processing" })
    end

    it "returns nil when not found" do
      allow(Pgbus::BatchRecord).to receive(:find_by).and_return(nil)

      expect(described_class.find("missing")).to be_nil
    end
  end

  describe ".cleanup" do
    it "deletes finished batches older than threshold" do
      scope = double("scope", delete_all: 3)
      allow(Pgbus::BatchRecord).to receive(:stale).and_return(scope)

      expect(described_class.cleanup(older_than: Time.now - 86_400)).to eq(3)
    end
  end
end
