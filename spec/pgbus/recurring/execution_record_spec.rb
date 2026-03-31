# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::RecurringExecutionRecord do
  describe ".record" do
    it "creates a record and yields" do
      yielded = false
      mock_record = double("execution", task_key: "test", run_at: Time.now)

      allow(described_class).to receive(:transaction).and_yield
      allow(described_class).to receive(:create!).and_return(mock_record)

      described_class.record("test", Time.now) { yielded = true }

      expect(yielded).to be true
    end

    it "raises AlreadyRecorded on duplicate" do
      allow(described_class).to receive(:transaction).and_yield
      allow(described_class).to receive(:create!)
        .and_raise(ActiveRecord::RecordNotUnique)

      expect do
        described_class.record("test", Time.now)
      end.to raise_error(Pgbus::Recurring::AlreadyRecorded)
    end
  end

  describe ".last_execution" do
    it "delegates to for_task scope" do
      relation = double("relation")
      ordered = double("ordered")

      allow(described_class).to receive(:for_task).with("my_task").and_return(relation)
      allow(relation).to receive(:order).with(run_at: :desc).and_return(ordered)
      allow(ordered).to receive(:first).and_return(nil)

      result = described_class.last_execution("my_task")
      expect(result).to be_nil
    end
  end
end
