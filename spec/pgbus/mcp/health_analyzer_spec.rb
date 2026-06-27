# frozen_string_literal: true

require_relative "spec_helper"

RSpec.describe Pgbus::MCP::HealthAnalyzer do
  subject(:analyzer) { described_class.new(data_source) }

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  def stub_data_source(queues: [], processes: [], health: {})
    allow(data_source).to receive_messages(queues_with_metrics: queues, processes: processes, queue_health_stats: health)
  end

  def queue(name:, length: 0, visible: 0, paused: false, max_read_ct: nil)
    h = { name: name, queue_length: length, queue_visible_length: visible, paused: paused }
    h[:max_read_ct] = max_read_ct unless max_read_ct.nil?
    h
  end

  def worker(status:, kind: "worker")
    { kind: kind, status: status }
  end

  describe "#verdict OK path" do
    it "returns OK when there is no backlog and workers are healthy" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 0, visible: 0)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end

    it "returns OK when a queue drains normally with a working worker" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 2)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons]).to be_empty
    end

    it "includes a machine-readable summary and ISO8601 checked_at" do
      stub_data_source(processes: [worker(status: :healthy)])

      verdict = analyzer.verdict
      expect(verdict[:summary]).to include(:queues, :workers, :total_visible)
      expect(verdict[:checked_at]).to match(/\A\d{4}-\d{2}-\d{2}T/)
    end
  end

  describe "#verdict STALLED path (silent wedge)" do
    it "is STALLED when a worker is stalled while a queue has visible backlog" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10)],
        processes: [worker(status: :stalled)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("stalled")
    end

    it "is STALLED when backlog is unread (read_ct 0) and workers are alive" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed")
    end

    it "treats backlog without read_ct metric as unread (conservative wedge default)" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 3, visible: 3)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "does not flag STALLED when there are no workers at all" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 3, visible: 3)],
        processes: []
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "ignores DLQ backlog for the wedge signal" do
      stub_data_source(
        queues: [queue(name: "pgbus_default_dlq", length: 5, visible: 5)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "does not flag STALLED when backlog has been read (read_ct > 0) and no worker is stalled" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 3)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end

  describe "#verdict DEGRADED path" do
    it "is DEGRADED when a process is stale" do
      stub_data_source(
        queues: [],
        processes: [worker(status: :stale)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].first).to include("stale")
    end

    it "is DEGRADED when a paused queue holds a backlog" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, paused: true, max_read_ct: 3)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("paused")
    end

    it "is DEGRADED when a dead-letter queue holds messages" do
      stub_data_source(
        queues: [queue(name: "pgbus_default_dlq", length: 4, visible: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("dead-letter")
    end

    it "is DEGRADED when the oldest transaction pins the MVCC horizon" do
      stub_data_source(
        queues: [],
        processes: [worker(status: :healthy)],
        health: { oldest_transaction_age_sec: 600 }
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("MVCC")
    end

    it "STALLED takes precedence over DEGRADED conditions" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0),
                 queue(name: "pgbus_default_dlq", length: 2, visible: 0)],
        processes: [worker(status: :stalled), worker(status: :stale)]
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end
  end

  describe "#verdict resilience" do
    it "tolerates queue_health_stats raising" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [])
      allow(data_source).to receive(:queue_health_stats).and_raise(StandardError)

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end
end
