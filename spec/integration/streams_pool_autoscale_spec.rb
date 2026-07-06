# frozen_string_literal: true

require_relative "../integration_helper"
require "concurrent"

# DB-gated proof that the self-tuning autoscaler grows the REAL streams pool
# under a real burst and shrinks it back — driving #tick against a real Client
# with the real HeadroomProbe reading live pg_stat_activity (issue #323).
# Auto-skips without PGBUS_DATABASE_URL. Modest sizes for a shared local DB.
RSpec.describe "Streams pool autoscaler", :integration do
  subject(:autoscaler) do
    Pgbus::Web::Streamer::PoolAutoscaler.new(client: client, config: config, logger: config.logger, clock: clock)
  end

  let(:database_url) { ENV.fetch("PGBUS_DATABASE_URL") }
  let(:stream_name)  { "as_#{SecureRandom.hex(4)}" }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_asspec"
      c.default_queue = "default"
      c.logger = Logger.new(IO::NULL)
      c.streams_pool_size = 3          # baseline / shrink floor
      c.streams_pool_max = 8           # modest hard cap for a shared DB
      c.streams_pool_timeout = 2
      c.streams_pool_autoscale = true
      c.streams_pool_autoscale_interval = 0.05
    end
  end

  let(:client) { Pgbus::Client.new(config, schema_ensured: true) }

  # Injectable clock so the test can fast-forward past the 15s post-swap cooldown
  # deterministically while all DB I/O (pool, probe, pg_stat_activity) stays real.
  let(:fake_now) { [0.0] }
  let(:clock) { -> { fake_now[0] } }

  def advance(seconds) = (fake_now[0] += seconds)

  before { client.ensure_stream_queue(stream_name) }
  after  { client.close }

  # Keep the streams pool busy with a CHURN of short checkouts (each ~30ms of
  # pg_sleep) across more threads than the pool has slots, so busy_ratio reads
  # high on most samples — but connections still cycle, so the HeadroomProbe can
  # slip a slot between ops (as it would in production, where checkouts are
  # 10-30ms, not held forever). Static holds would deadlock the probe: that is
  # the known residual risk, not the common case we autoscale for.
  def with_pool_churning(threads)
    stop = Concurrent::AtomicBoolean.new(false)
    started = Concurrent::CountDownLatch.new(threads)
    workers = Array.new(threads) do
      Thread.new do
        started.count_down
        until stop.true?
          begin
            client.send(:streams_pool).with_connection { |conn| conn.exec("SELECT pg_sleep(0.03)") }
          rescue StandardError
            # pool-timeout under contention is expected; keep churning
          end
        end
      end
    end
    started.wait(5)
    yield
  ensure
    stop&.make_true
    workers&.each { |t| t.join(5) }
  end

  it "grows the real streams pool under sustained saturation, then shrinks back to baseline" do
    # Saturate the pool so busy_ratio reads high on most samples; advance the
    # clock past each 15s post-swap cooldown so successive grows can fire.
    with_pool_churning(8) do
      grew = false
      60.times do
        grew ||= autoscaler.tick == :grow
        advance(20.0) # skip past any cooldown so the next sustained window can grow
        break if client.streams_pool_stats[:size] >= 6
      end
      expect(grew).to be true
      expect(client.streams_pool_stats[:size]).to be > 3
      expect(client.streams_pool_stats[:size]).to be <= 8 # respects the hard cap
    end

    # Load released → pool idle → sustained-low ticks → shrink back to baseline.
    200.times do
      autoscaler.tick
      advance(20.0) # past cooldown between shrink steps
      break if client.streams_pool_stats[:size] <= 3
    end
    expect(client.streams_pool_stats[:size]).to eq(3)
    expect(client.streams_swap_stats.swap_count).to be >= 2 # at least one grow + one shrink
  end

  it "does not error and stays within max_connections while growing (no PoolTimeout leak)" do
    with_pool_churning(8) do
      expect do
        30.times do
          autoscaler.tick
          advance(20.0)
        end
      end.not_to raise_error
    end
    # After growth the pool's open connections are bounded by the cap.
    expect(client.streams_pool_stats[:size]).to be <= 8
  end
end
