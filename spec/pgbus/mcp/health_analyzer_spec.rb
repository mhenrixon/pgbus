# frozen_string_literal: true

require_relative "spec_helper"

RSpec.describe Pgbus::MCP::HealthAnalyzer do
  subject(:analyzer) { described_class.new(data_source) }

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  # drained: nil (default) means "a wildcard capsule drains every queue", which
  # keeps the legacy specs — written before the drain-intersection existed —
  # asserting on the full backlog. Pass an explicit array to restrict the
  # drained set (issue #367 defect 2).
  def stub_data_source(queues: [], processes: [], health: {}, stream_queues: [], drained: nil)
    allow(data_source).to receive_messages(
      queues_with_metrics: queues, processes: processes,
      queue_health_stats: health, stream_queue_names: Set.new(stream_queues),
      drained_queue_names: drained.nil? ? nil : Set.new(drained)
    )
  end

  # visible_unread_length is the precise wedge signal: visible rows with
  # read_ct=0 (issue #367 review). When not given, it defaults from max_read_ct
  # so the legacy specs keep their meaning — a fully-unclaimed backlog
  # (max_read_ct 0 or absent) reports every visible row as unread; a claimed
  # one (max_read_ct > 0) reports none.
  def queue(name:, length: 0, visible: 0, paused: false, max_read_ct: nil, visible_unread: nil)
    h = { name: name, queue_length: length, queue_visible_length: visible, paused: paused }
    h[:max_read_ct] = max_read_ct unless max_read_ct.nil?
    h[:visible_unread_length] = visible_unread || (max_read_ct.to_i.zero? ? visible : 0)
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
    before { allow(Pgbus.logger).to receive(:warn) }

    it "still produces a verdict when queue_health_stats raises" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [],
                                             stream_queue_names: Set.new, drained_queue_names: nil)
      allow(data_source).to receive(:queue_health_stats).and_raise(StandardError, "boom")

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("queue health stats unavailable")
    end

    it "logs the queue_health_stats failure rather than swallowing it" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [],
                                             stream_queue_names: Set.new, drained_queue_names: nil)
      allow(data_source).to receive(:queue_health_stats).and_raise(StandardError, "boom")

      analyzer.verdict

      expect(Pgbus.logger).to have_received(:warn)
    end
  end

  describe "registered stream queues are excluded from the verdict (issue #359)" do
    it "does not read a stream queue's permanent read_ct=0 backlog as the wedge" do
      # Durable stream delivery is a non-consuming peek: visible messages with
      # read_ct=0 forever is the NORMAL state of a stream queue, not a wedge.
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons]).to be_empty
    end

    it "still detects a genuine job-queue wedge alongside noisy stream queues" do
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0),
                 queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("pgbus_default")
      expect(verdict[:reasons].first).not_to include("pgbus_chat_1_messages")
    end

    it "excludes stream backlog from the summary totals" do
      stub_data_source(
        queues: [queue(name: "pgbus_chat_1_messages", length: 10, visible: 10, max_read_ct: 0),
                 queue(name: "pgbus_default", length: 2, visible: 1, max_read_ct: 3)],
        processes: [worker(status: :healthy)],
        stream_queues: ["pgbus_chat_1_messages"]
      )

      summary = analyzer.verdict[:summary]
      expect(summary[:total_visible]).to eq(1)
      expect(summary[:total_depth]).to eq(2)
    end
  end

  describe "STALLED excludes paused queues" do
    it "classifies a paused queue with read_ct=0 backlog as DEGRADED, not STALLED" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, paused: true, max_read_ct: 0)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("paused")
    end
  end

  # Issue #367 defect 2: the wedge verdict must only reason about queues some
  # configured capsule (or EventBus handler) can actually drain. Live workers
  # prove nothing about a queue they could never claim from.
  describe "#verdict drain-intersection (issue #367 defect 2)" do
    it "does NOT flag STALLED for a backlog queue no capsule drains" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      expect(analyzer.verdict[:status]).not_to eq("STALLED")
    end

    it "reports an undrained backlog as DEGRADED with a 'no capsule' reason" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("DEGRADED")
      expect(verdict[:reasons].join).to include("no capsule")
      expect(verdict[:reasons].join).to include("pgbus_adhoc")
    end

    it "still flags STALLED for a drained queue while ignoring an undrained one" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5, max_read_ct: 0),
                 queue(name: "pgbus_adhoc", length: 9, visible: 9, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("pgbus_default")
      expect(verdict[:reasons].first).not_to include("pgbus_adhoc")
    end

    it "treats a wildcard capsule (drained_queue_names nil) as draining every queue" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: nil
      )

      expect(analyzer.verdict[:status]).to eq("STALLED")
    end

    it "counts a priority _pN sub-table as drained (drained set is physical-expanded)" do
      # A worker draining logical "critical" in priority mode drains the physical
      # pgbus_critical_p0.._pN tables; drained_queue_names returns those expanded
      # names, so a wedge on the priority sub-table is still caught as STALLED.
      stub_data_source(
        queues: [queue(name: "pgbus_critical_p0", length: 5, visible: 5, max_read_ct: 0)],
        processes: [worker(status: :healthy)],
        drained: %w[pgbus_critical_p0 pgbus_critical_p1]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].join).not_to include("no capsule")
    end

    it "does not report an undrained-queue DEGRADED when the queue has been claimed" do
      stub_data_source(
        queues: [queue(name: "pgbus_adhoc", length: 5, visible: 5, max_read_ct: 4)],
        processes: [worker(status: :healthy)],
        drained: ["pgbus_default"]
      )

      # A queue with progress (read_ct > 0) that nobody drains isn't wedged and
      # isn't the "no capsule configured" hazard — something is claiming it.
      expect(analyzer.verdict[:reasons].join).not_to include("no capsule")
    end
  end

  # Issue #367 defect 1: max_read_ct is now populated, so read_ct > 0 must NOT
  # produce the "read_ct=0 (never claimed)" reason string.
  describe "#verdict does not assert 'never claimed' about claimed messages (issue #367 defect 1)" do
    it "is OK (not STALLED) when a drained queue's backlog has been read mid-burst" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 10, visible: 10, max_read_ct: 1)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("OK")
      expect(verdict[:reasons].join).not_to include("never claimed")
    end
  end

  # Issue #367 review: the wedge signal must count VISIBLE never-claimed rows,
  # not max(read_ct) over the whole table — one retried/in-flight message
  # (read_ct > 0) must not veto the signal for a pile of visible read_ct=0 jobs.
  describe "#verdict partial-wedge detection (issue #367 review)" do
    it "is STALLED when visible read_ct=0 jobs pile up despite a lingering retried message" do
      # 500 fresh unclaimed jobs (the wedge) + 1 message that retried once, so
      # max_read_ct=1 but 500 visible rows have never been claimed.
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 501, visible: 501,
                       max_read_ct: 1, visible_unread: 500)],
        processes: [worker(status: :healthy)]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("never claimed")
    end

    it "is OK when every visible message has been claimed (visible_unread=0) even if some are read once" do
      stub_data_source(
        queues: [queue(name: "pgbus_default", length: 5, visible: 5,
                       max_read_ct: 2, visible_unread: 0)],
        processes: [worker(status: :healthy)]
      )

      expect(analyzer.verdict[:status]).to eq("OK")
    end
  end

  # Issue #367 review: a heart-beating-but-stalled worker is a wedge regardless
  # of WHICH queue holds the visible backlog — the stalled-worker branch must
  # run against the full active backlog, not only the drained partition.
  describe "#verdict stalled-worker precedence across undrained backlog (issue #367 review)" do
    it "is STALLED (not DEGRADED) when a worker is stalled and the backlog is on an undrained queue" do
      stub_data_source(
        queues: [queue(name: "pgbus_y", length: 8, visible: 8, max_read_ct: 0)],
        processes: [worker(status: :stalled)],
        drained: ["pgbus_x"]
      )

      verdict = analyzer.verdict
      expect(verdict[:status]).to eq("STALLED")
      expect(verdict[:reasons].first).to include("stalled")
    end
  end

  # Issue #367 minor: a reason string listing hundreds of queues is a multi-KB
  # log line. Truncate to the first ~10 with a (+N more) suffix.
  describe "#verdict truncates long backlog name lists (issue #367 minor)" do
    it "lists at most 10 queue names and appends (+N more)" do
      queues = (1..25).map { |i| queue(name: "pgbus_q#{i}", length: 2, visible: 2, max_read_ct: 0) }
      stub_data_source(queues: queues, processes: [worker(status: :healthy)])

      reason = analyzer.verdict[:reasons].first
      expect(reason).to include("(+15 more)")
      expect(reason.scan(/pgbus_q\d+/).size).to eq(10)
    end
  end
end
