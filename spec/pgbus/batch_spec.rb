# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Batch do
  let(:connection) { double("AR::Connection") }

  before do
    stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
    allow(connection).to receive(:exec_query).and_return([])
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

      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/INSERT INTO pgbus_batches/),
        "Pgbus Batch Create",
        array_including(batch.batch_id, "test")
      )
    end

    it "updates total_jobs after counting" do
      batch = described_class.new
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/UPDATE pgbus_batches SET total_jobs/),
        "Pgbus Batch Update Total",
        anything
      )
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
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/completed_jobs = completed_jobs \+ 1/), anything, anything)
        .and_return(result)

      described_class.job_completed("batch-123")

      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/UPDATE pgbus_batches/),
        "Pgbus Batch Counter",
        ["batch-123"]
      )
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
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/UPDATE pgbus_batches/), "Pgbus Batch Counter", anything)
        .and_return(result)

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
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/UPDATE pgbus_batches/), "Pgbus Batch Counter", anything)
        .and_return(result)

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
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/UPDATE pgbus_batches/), "Pgbus Batch Counter", anything)
        .and_return(result)

      callback_job = class_double("DiscardJob", perform_later: nil) # rubocop:disable RSpec/VerifiedDoubleReference
      stub_const("DiscardJob", callback_job)

      described_class.job_discarded("batch-123")

      expect(callback_job).to have_received(:perform_later)
    end

    it "returns nil when batch not found" do
      result = double("Result", first: nil)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/UPDATE pgbus_batches/), anything, anything)
        .and_return(result)

      expect(described_class.job_completed("nonexistent")).to be_nil
    end
  end

  describe ".find" do
    it "returns the batch record" do
      row = { "batch_id" => "abc", "status" => "processing" }
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/SELECT .* FROM pgbus_batches/), anything, anything)
        .and_return(result)

      expect(described_class.find("abc")).to eq(row)
    end
  end

  describe ".cleanup" do
    it "deletes finished batches older than threshold" do
      result = double("Result", to_a: [{ "id" => 1 }])
      allow(connection).to receive(:exec_query)
        .with(a_string_matching(/DELETE FROM pgbus_batches/), anything, anything)
        .and_return(result)

      expect(described_class.cleanup(older_than: Time.now - 86_400)).to eq(1)
    end
  end

  context "when ActiveRecord is not defined" do
    before { hide_const("ActiveRecord") }

    it "job_completed returns nil" do
      expect(described_class.job_completed("batch-123")).to be_nil
    end

    it "find returns nil" do
      expect(described_class.find("batch-123")).to be_nil
    end

    it "cleanup returns 0" do
      expect(described_class.cleanup(older_than: Time.now)).to eq(0)
    end
  end
end
