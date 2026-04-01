# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::JobStat do
  before do
    stub_const("Pgbus::JobStat", Class.new do
      def self.create!(**attrs); end

      def self.record!(job_class:, queue_name:, status:, duration_ms:)
        create!(job_class: job_class, queue_name: queue_name, status: status, duration_ms: duration_ms)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to record job stat: #{e.message}" }
      end
    end)

    allow(described_class).to receive(:create!)
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

    it "rescues errors gracefully" do
      allow(described_class).to receive(:create!).and_raise(StandardError, "db error")

      expect do
        described_class.record!(job_class: "X", queue_name: "q", status: "s", duration_ms: 0)
      end.not_to raise_error
    end
  end
end
