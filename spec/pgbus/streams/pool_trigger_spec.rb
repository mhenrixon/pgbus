# frozen_string_literal: true

require "spec_helper"

# Publisher-side throttled autoscale trigger (issue #323 follow-up). Fires a
# headroom check at most once per `interval` seconds — across concurrent
# publisher threads — DEFERRED to a background executor so the publish thread
# never runs the query. Runs through the JOB pool (not the streams pool, so it
# can't self-starve) with allow_shrink: false. Fail-soft.
RSpec.describe Pgbus::Streams::PoolTrigger do
  subject(:trigger) do
    described_class.new(autoscaler: autoscaler, job_pool: job_pool, interval: 300,
                        application_name_prefix: "pgbus_streams", clock: clock, executor: executor)
  end

  # A synchronous "executor" so posted work runs inline — deterministic tests
  # without threading. Mirrors the Concurrent executor's #post/#shutdown surface.
  let(:executor) do
    Class.new do
      def post(&blk) = blk.call
      def shutdown; end
      def wait_for_termination(_timeout) = true
    end.new
  end

  let(:autoscaler) { instance_double(Pgbus::Streams::PoolAutoscaler, evaluate: :hold) }
  let(:now) { [0.0] }
  let(:clock) { -> { now[0] } }

  let(:row) { { "maxc" => "100", "used" => "40", "peers" => "5" } }
  let(:conn) do
    c = double("PG::Connection")
    result = double("PG::Result", first: row)
    allow(c).to receive(:exec_params).and_return(result)
    c
  end
  let(:job_pool) do
    p = double("job_pool")
    allow(p).to receive(:with_connection).and_yield(conn)
    p
  end

  describe "#maybe_check" do
    it "runs a headroom check and evaluates grow-only (allow_shrink: false)" do
      trigger.maybe_check
      expect(autoscaler).to have_received(:evaluate).with({ maxc: 100, used: 40, peers: 5 }, allow_shrink: false)
    end

    it "queries through the job pool, not the streams pool" do
      trigger.maybe_check
      expect(job_pool).to have_received(:with_connection)
      expect(conn).to have_received(:exec_params)
        .with(Pgbus::Streams::PoolAutoscaler::HEADROOM_SQL, ["pgbus_streams_%"])
    end

    it "defers the work to the executor (does not run inline on the publish thread)" do
      posted = false
      allow(executor).to receive(:post) { posted = true }
      trigger.maybe_check
      expect(posted).to be true
    end

    it "throttles: a second call within the interval does not re-check" do
      trigger.maybe_check
      now[0] += 100 # still inside the 300s interval
      trigger.maybe_check
      expect(autoscaler).to have_received(:evaluate).once
    end

    it "checks again once the interval has elapsed" do
      trigger.maybe_check
      now[0] += 301
      trigger.maybe_check
      expect(autoscaler).to have_received(:evaluate).twice
    end

    it "is fail-soft: a raising query never propagates" do
      allow(conn).to receive(:exec_params).and_raise(StandardError, "boom")
      expect { trigger.maybe_check }.not_to raise_error
    end

    it "does not post when the throttle window is not open" do
      trigger.maybe_check # opens + posts
      allow(executor).to receive(:post)
      now[0] += 10 # inside interval
      trigger.maybe_check
      expect(executor).not_to have_received(:post)
    end

    it "runs at most one check per window across concurrent threads" do
      real_executor = Class.new do
        def post(&blk) = blk.call
        def shutdown; end
      end.new
      concurrent_trigger = described_class.new(
        autoscaler: autoscaler, job_pool: job_pool, interval: 300,
        application_name_prefix: "pgbus_streams", clock: clock, executor: real_executor
      )
      allow(job_pool).to receive(:with_connection) do |&blk|
        sleep 0.01
        blk.call(conn)
      end

      threads = Array.new(10) { Thread.new { concurrent_trigger.maybe_check } }
      threads.each(&:join)

      expect(autoscaler).to have_received(:evaluate).once
    end
  end

  describe "#shutdown" do
    it "shuts the executor down (idempotent)" do
      allow(executor).to receive(:shutdown)
      trigger.shutdown
      trigger.shutdown
      expect(executor).to have_received(:shutdown).twice
    end
  end

  describe "the default background executor" do
    # Guards the real defect the concurrency review found: SingleThreadExecutor
    # silently ignores max_queue:, so a genuinely bounded 1-slot queue needs the
    # base ThreadPoolExecutor. Build a real trigger (no executor: injected) and
    # assert the executor honors its bound so overflow can actually discard.
    subject(:real_trigger) do
      described_class.new(autoscaler: autoscaler, job_pool: job_pool, interval: 300,
                          application_name_prefix: "pgbus_streams", clock: clock)
    end

    after { real_trigger.shutdown }

    it "uses a genuinely bounded 1-slot queue (max_queue honored, overflow can discard)" do
      exec = real_trigger.instance_variable_get(:@executor)
      expect(exec.max_queue).to eq(1)
      expect(exec.can_overflow?).to be true # false would mean an unbounded queue
    end
  end
end
