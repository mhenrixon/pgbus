# frozen_string_literal: true

require_relative "../integration_helper"
require "concurrent"

# DB-gated proof that the self-tuning autoscaler grows the REAL streams pool
# under saturation and shrinks it back — driving #evaluate with headroom read
# from live pg_stat_activity via the Maintenance query (issue #323). The
# autoscaler is a pure decision object here; the periodic scheduling (via the
# Listener's idle window) is covered by the unit specs. Auto-skips without
# PGBUS_DATABASE_URL. Modest sizes for a shared local DB.
RSpec.describe "Streams pool autoscaler", :integration do
  let(:database_url) { ENV.fetch("PGBUS_DATABASE_URL") }
  let(:stream_name)  { "as_#{SecureRandom.hex(4)}" }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_asspec"
      c.default_queue = "default"
      c.logger = Logger.new(IO::NULL)
      c.streams_pool_size = 3    # baseline / shrink floor
      c.streams_pool_max = 8     # modest hard cap for a shared DB
      c.streams_pool_timeout = 2
    end
  end

  let(:client) { Pgbus::Client.new(config, schema_ensured: true) }
  let(:autoscaler) { Pgbus::Streams::PoolAutoscaler.new(client: client, config: config, logger: config.logger) }
  let(:maintenance) do
    Pgbus::Streams::PoolAutoscaler::Maintenance.new(
      autoscaler: autoscaler, interval: 0, application_name_prefix: config.streams_application_name
    )
  end

  before { client.ensure_stream_queue(stream_name) }
  after  { client.close }

  # Read live headroom the same way the Listener would (a separate connection
  # stands in for the Listener's idle LISTEN connection) and run one decision.
  def one_check
    conn = PG.connect(database_url)
    maintenance.run(conn)
  ensure
    conn&.close
  end

  # Hold the whole baseline pool so busy_ratio == 1.0. The decision reads
  # streams_pool_stats (no checkout), so it is not starved by the held slots.
  def with_pool_saturated(hold)
    barrier = Concurrent::CountDownLatch.new(hold)
    release = Concurrent::CountDownLatch.new(1)
    holders = Array.new(hold) do
      Thread.new do
        client.send(:streams_pool).with_connection do |_c|
          barrier.count_down
          release.wait(10)
        end
      rescue StandardError
        barrier.count_down
      end
    end
    barrier.wait(5)
    yield
  ensure
    release&.count_down
    holders&.each { |t| t.join(5) }
  end

  it "grows the real streams pool under saturation, capped, then shrinks back to baseline" do
    with_pool_saturated(3) do
      # A few checks (each a real headroom query + one grow step) → grows to cap.
      grew = false
      6.times do
        grew ||= one_check == :grow
        break if client.streams_pool_stats[:size] >= 8
      end
      expect(grew).to be true
      expect(client.streams_pool_stats[:size]).to be > 3
      expect(client.streams_pool_stats[:size]).to be <= 8 # hard cap
    end

    # Load released → idle → each check shrinks one step back to baseline.
    10.times do
      one_check
      break if client.streams_pool_stats[:size] <= 3
    end
    expect(client.streams_pool_stats[:size]).to eq(3)
    expect(client.streams_swap_stats.swap_count).to be >= 2 # ≥1 grow + ≥1 shrink
  end

  it "emergency-shrinks to baseline when the DB is nearly out of connections" do
    # Grow first so there is something to shrink.
    with_pool_saturated(3) do
      3.times do
        one_check
        break if client.streams_pool_stats[:size] > 3
      end
    end
    expect(client.streams_pool_stats[:size]).to be > 3

    # Simulate near-exhaustion by feeding a headroom reading with tiny free.
    grown = client.streams_pool_stats[:size]
    action = autoscaler.evaluate(maxc: 100, used: 99, peers: 1) # free=1 < emergency margin
    expect(action).to eq(:emergency_shrink)
    expect(client.streams_pool_stats[:size]).to eq(3)
    expect(grown).to be > 3
  end
end
