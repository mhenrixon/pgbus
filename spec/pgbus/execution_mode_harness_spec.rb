# frozen_string_literal: true

require "spec_helper"
require "concurrent"
require_relative "../../benchmarks/support/execution_mode_harness"

# Unit specs for the execution-mode benchmark harness (issue #323). These run
# WITHOUT a database — the fairness/struct/measurement LOGIC is tested with
# injected fakes. The actual connection/throughput NUMBERS come from a manual
# `rake bench:execution_modes` run against a real Postgres (run-and-report,
# never a CI gate).
RSpec.describe ExecutionModeHarness do
  describe ExecutionModeHarness::IoProfile do
    it "io_light spends most time OUTSIDE the checkout (fiber-shareable)" do
      p = described_class.io_light
      expect(p.label).to eq("io_light")
      expect(p.yield_seconds).to be > p.db_seconds
    end

    it "db_bound spends most time INSIDE the checkout (connection-bound)" do
      p = described_class.db_bound
      expect(p.label).to eq("db_bound")
      expect(p.db_seconds).to be > p.yield_seconds
    end

    it "keeps db_seconds at or above 10ms so the 5ms sampler can't undercount the checkout" do
      [described_class.io_light, described_class.db_bound].each do |p|
        expect(p.db_seconds).to be >= 0.010
      end
    end
  end

  describe ExecutionModeHarness::Result do
    subject(:result) do
      described_class.new(
        mode: :threads, pool_size: 12, concurrency: 12,
        io_profile: ExecutionModeHarness::IoProfile.io_light,
        job_count: 240, peak_busy: 9, steady_busy: 4,
        throughput: 123.45, p50: 41.0, p95: 60.0, p99: 90.0,
        pool_timeouts: 0, other_errors: 0, completed: 240, wall_seconds: 1.94
      )
    end

    it "is under_provisioned? only when pool_size < concurrency" do
      expect(result.under_provisioned?).to be false
      smaller = result.with(pool_size: 3)
      expect(smaller.under_provisioned?).to be true
    end

    it "produces a row whose width matches the headers" do
      expect(result.to_row.length).to eq(described_class.headers.length)
    end
  end

  describe ".summarize" do
    it "computes throughput as completed / elapsed and correct percentiles" do
      latencies = (1..100).map(&:to_f) # sorted 1..100
      outcome = ExecutionModeHarness::Outcome.new(
        latencies: latencies, elapsed: 2.0, finished: true,
        completed: 100, pool_timeouts: 0, other_errors: 0
      )
      sampler = instance_double(ExecutionModeHarness::PoolSampler,
                                summary: { peak_busy: 9, steady_busy: 4 })

      result = described_class.summarize(
        mode: :threads, pool_size: 12, concurrency: 12,
        io_profile: ExecutionModeHarness::IoProfile.io_light,
        job_count: 100, sampler: sampler, outcome: outcome
      )

      expect(result.throughput).to eq(50.0) # 100 / 2.0
      expect(result.p50).to be_within(2.0).of(50.0)
      expect(result.p95).to be_within(2.0).of(95.0)
      expect(result.p99).to be_within(2.0).of(99.0)
      expect(result.peak_busy).to eq(9)
      expect(result.steady_busy).to eq(4)
    end
  end

  describe ".assert_fair_pair!" do
    def result(mode:, job_count: 240, io: ExecutionModeHarness::IoProfile.io_light, concurrency: 12)
      ExecutionModeHarness::Result.new(
        mode: mode, pool_size: 12, concurrency: concurrency, io_profile: io,
        job_count: job_count, peak_busy: 1, steady_busy: 1, throughput: 1.0,
        p50: 1.0, p95: 1.0, p99: 1.0, pool_timeouts: 0, other_errors: 0,
        completed: job_count, wall_seconds: 1.0
      )
    end

    it "passes when offered load (job_count, io_profile, concurrency) matches" do
      expect do
        described_class.assert_fair_pair!(result(mode: :threads), result(mode: :async))
      end.not_to raise_error
    end

    it "raises when job_count differs (the anti-rigging guard)" do
      expect do
        described_class.assert_fair_pair!(result(mode: :threads, job_count: 20),
                                          result(mode: :async, job_count: 200))
      end.to raise_error(ArgumentError, /unfair/)
    end

    it "raises when io_profile differs" do
      expect do
        described_class.assert_fair_pair!(
          result(mode: :threads, io: ExecutionModeHarness::IoProfile.io_light),
          result(mode: :async, io: ExecutionModeHarness::IoProfile.db_bound)
        )
      end.to raise_error(ArgumentError, /unfair/)
    end
  end

  describe ".classify_error!" do
    let(:timeouts) { Concurrent::AtomicFixnum.new(0) }
    let(:others)   { Concurrent::AtomicFixnum.new(0) }

    it "tallies a pool-timeout error by its message marker (never swallows)" do
      # classify_error! keys off the message substring (POOL_TIMEOUT_MARKER),
      # mirroring production's pool_timeout_error?, so the real PGMQ error class
      # (not loaded in unit tests) isn't needed — only the marker text.
      err = StandardError.new("Connection pool timeout: waited 5s")
      described_class.classify_error!(err, timeouts, others)
      expect(timeouts.value).to eq(1)
      expect(others.value).to eq(0)
    end

    it "tallies a non-timeout error as other_errors" do
      described_class.classify_error!(StandardError.new("boom"), timeouts, others)
      expect(timeouts.value).to eq(0)
      expect(others.value).to eq(1)
    end
  end

  describe ".assert_dedicated_path!" do
    it "raises on the shared-AR path (pool_size forced to 1)" do
      client = instance_double(Pgbus::Client, shared_connection?: true)
      expect { described_class.assert_dedicated_path!(client) }
        .to raise_error(ArgumentError, /dedicated-connection path/)
    end

    it "passes on the dedicated path" do
      client = instance_double(Pgbus::Client, shared_connection?: false)
      expect { described_class.assert_dedicated_path!(client) }.not_to raise_error
    end
  end

  describe ".run_job" do
    # A plain Kernel#sleep is used for the yield portion — under Async's fiber
    # scheduler it's intercepted and yields to the reactor; under threads it
    # blocks the thread. No mode-specific branch needed.
    it "sleeps for the yield portion outside any checkout" do
      allow(described_class).to receive(:sleep)
      io = ExecutionModeHarness::IoProfile.new(label: "yield_only", db_seconds: 0.0, yield_seconds: 0.02)
      client = instance_double(Pgbus::Client) # never touched: db_seconds is 0

      described_class.run_job(client, io)

      expect(described_class).to have_received(:sleep).with(0.02)
    end

    it "checks out a real pooled connection for the db portion" do
      # PG / PGMQ::Client aren't loaded in unit specs (pgmq is stubbed away), so
      # use plain doubles for the conn + pgmq; the harness only calls
      # #with_connection and #exec_params on them.
      allow(described_class).to receive(:sleep)
      conn = double("PG::Connection", exec_params: nil)
      pgmq = double("PGMQ::Client")
      allow(pgmq).to receive(:with_connection).and_yield(conn)
      client = instance_double(Pgbus::Client, pgmq: pgmq)
      io = ExecutionModeHarness::IoProfile.new(label: "db_only", db_seconds: 0.01, yield_seconds: 0.0)

      described_class.run_job(client, io)

      expect(pgmq).to have_received(:with_connection)
      expect(conn).to have_received(:exec_params).with("SELECT pg_sleep($1)", [0.01])
    end
  end

  describe ExecutionModeHarness::PoolSampler do
    subject(:sampler) { described_class.new(client, interval_s: 0.001) }

    # A client whose pool_stats returns a scripted sequence, so busy =
    # size - available and the median math are provable with no DB.
    let(:client) do
      stats = [
        { size: 12, available: 12, pool_timeout: 5 }, # busy 0
        { size: 12, available: 3,  pool_timeout: 5 }, # busy 9  (peak)
        { size: 12, available: 8,  pool_timeout: 5 }  # busy 4
      ]
      instance_double(Pgbus::Client).tap do |c|
        seq = stats.dup
        allow(c).to receive(:pool_stats) { seq.shift || stats.last }
      end
    end

    it "reports peak_busy and median steady_busy from size - available" do
      # Drive the sampler deterministically over exactly the scripted stats.
      sampler.record(client.pool_stats)
      sampler.record(client.pool_stats)
      sampler.record(client.pool_stats)

      summary = sampler.summary
      expect(summary[:peak_busy]).to eq(9)
      expect(summary[:steady_busy]).to eq(4) # median([0,9,4])
    end

    it "skips a degraded ({}) sample instead of counting it as busy: 0" do
      sampler.record({})
      sampler.record({ size: 10, available: 2 }) # busy 8

      summary = sampler.summary
      expect(summary[:peak_busy]).to eq(8)
      expect(summary[:steady_busy]).to eq(8) # only the one real sample
    end
  end
end
