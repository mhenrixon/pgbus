# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::StatBuffer do
  subject(:buffer) { described_class.new(flush_size: 3, flush_interval: 0.1) }

  let(:stat_attrs) do
    {
      job_class: "TestJob",
      queue_name: "default",
      status: "success",
      duration_ms: 42,
      enqueue_latency_ms: 10,
      retry_count: 0
    }
  end

  before do
    stub_const("Pgbus::JobStat", Class.new) unless defined?(Pgbus::JobStat)
    allow(Pgbus::JobStat).to receive_messages(table_exists?: true, latency_columns?: true, insert_all: nil)
  end

  describe "#push" do
    it "accumulates entries in the buffer" do
      buffer.push(stat_attrs)
      expect(buffer.size).to eq(1)
    end

    it "auto-flushes when flush_size is reached" do
      3.times { buffer.push(stat_attrs) }

      expect(Pgbus::JobStat).to have_received(:insert_all)
      expect(buffer.size).to eq(0)
    end

    it "does not flush before reaching flush_size" do
      2.times { buffer.push(stat_attrs) }

      expect(Pgbus::JobStat).not_to have_received(:insert_all)
      expect(buffer.size).to eq(2)
    end
  end

  describe "#flush" do
    it "writes buffered entries to the database" do
      2.times { buffer.push(stat_attrs) }
      buffer.flush

      expect(Pgbus::JobStat).to have_received(:insert_all).with(
        array_including(hash_including(job_class: "TestJob")),
        record_timestamps: true
      )
      expect(buffer.size).to eq(0)
    end

    it "is a no-op when buffer is empty" do
      buffer.flush

      expect(Pgbus::JobStat).not_to have_received(:insert_all)
    end

    it "rescues database errors without raising" do
      allow(Pgbus::JobStat).to receive(:insert_all).and_raise(StandardError, "db error")

      buffer.push(stat_attrs)
      expect { buffer.flush }.not_to raise_error
    end
  end

  describe "#flush_if_due" do
    it "flushes when interval has elapsed" do
      buffer.push(stat_attrs)
      sleep(0.15) # exceed 0.1s flush_interval
      buffer.flush_if_due

      expect(Pgbus::JobStat).to have_received(:insert_all)
    end

    it "does not flush before interval elapses" do
      buffer.push(stat_attrs)
      buffer.flush_if_due

      expect(Pgbus::JobStat).not_to have_received(:insert_all)
    end
  end

  describe "#stop" do
    it "flushes remaining entries" do
      buffer.push(stat_attrs)
      buffer.stop

      expect(Pgbus::JobStat).to have_received(:insert_all)
      expect(buffer.size).to eq(0)
    end
  end

  describe "without latency columns" do
    before { allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(false) }

    it "omits latency fields from the insert" do
      buffer.push(stat_attrs)
      buffer.flush

      expect(Pgbus::JobStat).to have_received(:insert_all) do |rows, **_opts|
        row = rows.first
        expect(row).to include(job_class: "TestJob", duration_ms: 42)
        expect(row).not_to have_key(:enqueue_latency_ms)
        expect(row).not_to have_key(:retry_count)
      end
    end
  end
end
