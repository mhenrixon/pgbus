# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::JobStat do
  before do
    allow(described_class).to receive_messages(create!: nil, table_exists?: true)
  end

  describe ".record!" do
    it "creates a stat record" do
      described_class.record!(
        job_class: "TestJob",
        queue_name: "default",
        status: "success",
        duration_ms: 42
      )

      expect(described_class).to have_received(:create!).with(
        job_class: "TestJob",
        queue_name: "default",
        status: "success",
        duration_ms: 42
      )
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
end
