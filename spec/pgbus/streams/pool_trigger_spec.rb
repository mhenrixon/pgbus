# frozen_string_literal: true

require "spec_helper"

# Publisher-side throttled autoscale trigger (issue #323 follow-up). Fires a
# headroom check at most once per `interval` seconds — across concurrent
# publisher threads — running the query through the JOB pool (not the streams
# pool, so it can't self-starve) and handing the reading to a PoolAutoscaler.
# Fail-soft: never raises into the publish path.
RSpec.describe Pgbus::Streams::PoolTrigger do
  subject(:trigger) do
    described_class.new(autoscaler: autoscaler, job_pool: job_pool, interval: 300,
                        application_name_prefix: "pgbus_streams", clock: clock)
  end

  let(:autoscaler) { instance_double(Pgbus::Streams::PoolAutoscaler, evaluate: :hold) }
  let(:now) { [0.0] }
  let(:clock) { -> { now[0] } }

  # Fake job pool: with_connection yields a fake PG connection that answers the
  # headroom query.
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
    it "runs a headroom check and evaluates on the first call" do
      trigger.maybe_check
      expect(autoscaler).to have_received(:evaluate).with(maxc: 100, used: 40, peers: 5)
    end

    it "queries through the job pool, not the streams pool" do
      trigger.maybe_check
      expect(job_pool).to have_received(:with_connection)
      expect(conn).to have_received(:exec_params)
        .with(Pgbus::Streams::PoolAutoscaler::HEADROOM_SQL, ["pgbus_streams_%"])
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

    it "runs at most one check per window across concurrent threads" do
      allow(job_pool).to receive(:with_connection) do |&blk|
        sleep 0.01 # widen the race window
        blk.call(conn)
      end

      threads = Array.new(10) { Thread.new { trigger.maybe_check } }
      threads.each(&:join)

      # Only one thread should have won the window and run the check.
      expect(autoscaler).to have_received(:evaluate).once
    end
  end
end
