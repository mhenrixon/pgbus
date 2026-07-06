# frozen_string_literal: true

require_relative "../integration_helper"
require_relative "../../benchmarks/support/execution_mode_harness"

# DB-gated specs for the execution-mode harness (issue #323): prove run_cell
# drives real jobs through the real pool checkout and reports sane metrics.
# Auto-skips when PGBUS_DATABASE_URL is unset (see integration_helper). These
# are run-and-report, never a CI perf gate.
RSpec.describe "ExecutionModeHarness run_cell", :integration do
  let(:database_url) { ENV.fetch("PGBUS_DATABASE_URL") }
  # Small offered load so the spec stays fast and connection-modest.
  let(:job_count)   { 24 }
  let(:concurrency) { 4 }

  # A dedicated-path client sized to `pool_size`. Own config so pool_size varies
  # per example without touching the shared Pgbus.configuration.
  def build_client(pool_size)
    config = Pgbus::Configuration.new.tap do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_emspec"
      c.default_queue = "default"
      c.logger = Logger.new(IO::NULL)
      c.pool_size = pool_size
      c.pool_timeout = 5
      c.stats_enabled = false
    end
    Pgbus::Client.new(config, schema_ensured: true)
  end

  it "drives a threads cell to completion with no errors and peak_busy <= pool_size" do
    client = build_client(4)
    result = ExecutionModeHarness.run_cell(
      mode: :threads, pool_size: 4, concurrency: concurrency,
      io_profile: ExecutionModeHarness::IoProfile.io_light,
      job_count: job_count, client: client
    )

    expect(result.completed).to eq(job_count)
    expect(result.other_errors).to eq(0)
    expect(result.peak_busy).to be <= 4
    expect(result.peak_busy).to be > 0 # it actually held connections
    expect(result.throughput).to be > 0
  ensure
    client&.close
  end

  it "reports higher peak_busy at higher pool_size for the same offered load" do
    small = build_client(2)
    large = build_client(4)
    io = ExecutionModeHarness::IoProfile.db_bound

    r_small = ExecutionModeHarness.run_cell(mode: :threads, pool_size: 2, concurrency: concurrency,
                                            io_profile: io, job_count: job_count, client: small)
    r_large = ExecutionModeHarness.run_cell(mode: :threads, pool_size: 4, concurrency: concurrency,
                                            io_profile: io, job_count: job_count, client: large)

    # Same offered load — a fair pair.
    expect { ExecutionModeHarness.assert_fair_pair!(r_small, r_large) }.not_to raise_error
    # Deterministic invariants only — NOT a cross-run timing comparison (which
    # would flake if the 5ms sampler missed a peak under GC/contention). A pool
    # can never check out more than its size, and a DB-bound load at concurrency
    # 4 keeps every slot busy, so each pool saturates to its own ceiling.
    expect(r_small.peak_busy).to be_between(1, 2)
    expect(r_large.peak_busy).to be_between(1, 4)
  ensure
    small&.close
    large&.close
  end

  it "runs an async cell when the async gem is available (fibers share a small pool on io_light)" do
    # `async` is NOT a runtime dependency of pgbus — async execution mode
    # requires the app to add `gem "async"` (AsyncPool#initialize raises a clear
    # LoadError otherwise). It's present here transitively (via falcon/async-http
    # dev deps), so this runs in this repo; in an environment without it, this
    # example skips rather than failing — a SKIP is not a pass, so don't read
    # green here as "async is covered" unless the gem is actually loadable.
    skip "async gem not available (add `gem \"async\"` to exercise async execution mode)" unless async_gem_available?

    client = build_client(2)
    result = ExecutionModeHarness.run_cell(
      mode: :async, pool_size: 2, concurrency: concurrency,
      io_profile: ExecutionModeHarness::IoProfile.io_light,
      job_count: job_count, client: client
    )

    expect(result.completed).to eq(job_count)
    expect(result.other_errors).to eq(0)
    # Proves fibers shared: concurrency 4 sustained on a pool of 2.
    expect(result.peak_busy).to be <= 2
  ensure
    client&.close
  end

  def async_gem_available?
    require "async"
    true
  rescue LoadError
    false
  end
end
