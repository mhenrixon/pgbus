# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::JobStat do
  before do
    allow(described_class).to receive_messages(create!: nil, table_exists?: true)
  end

  describe ".record!" do
    context "without latency columns" do
      before { allow(described_class).to receive(:latency_columns?).and_return(false) }

      it "creates a stat record without latency fields" do
        described_class.record!(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 1
        )

        expect(described_class).to have_received(:create!).with(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42
        )
      end
    end

    context "with latency columns" do
      before { allow(described_class).to receive(:latency_columns?).and_return(true) }

      it "creates a stat record with latency fields" do
        described_class.record!(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 2
        )

        expect(described_class).to have_received(:create!).with(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 2
        )
      end
    end

    it "skips when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!(job_class: "X", queue_name: "q", status: "s", duration_ms: 0)

      expect(described_class).not_to have_received(:create!)
    end

    it "rescues errors gracefully" do
      allow(described_class).to receive(:create!).and_raise(StandardError, "db error")

      expect do
        described_class.record!(job_class: "X", queue_name: "q", status: "s", duration_ms: 0)
      end.not_to raise_error
    end
  end

  describe ".latency_columns?" do
    before { described_class.remove_instance_variable(:@latency_columns) if described_class.instance_variable_defined?(:@latency_columns) }

    it "returns false when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)
      expect(described_class.latency_columns?).to be false
    end
  end

  describe ".latency_trend" do
    it "returns empty array when latency columns are not available" do
      allow(described_class).to receive(:latency_columns?).and_return(false)
      expect(described_class.latency_trend).to eq([])
    end
  end

  describe ".avg_latency_by_queue" do
    it "returns empty hash when latency columns are not available" do
      allow(described_class).to receive(:latency_columns?).and_return(false)
      expect(described_class.avg_latency_by_queue).to eq({})
    end
  end
end
