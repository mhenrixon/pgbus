# frozen_string_literal: true

# Streams-pool autoscaler benchmark (issue #323).
#
# Measures the self-tuning control loop against a real Postgres:
#   (1) tick overhead — the cost of one decision (headroom probe read + policy),
#       so we can assert it is cheap enough to run every second;
#   (2) grow-under-load — that a sustained DB-bound streams burst on a small
#       fixed pool grows the pool into headroom, with the swaps bounded by the cap.
#
# HONEST FRAMING: this measures the autoscaler's own cost + that it reacts, NOT
# a per-op speedup. Whether autoscaling beats a well-sized static streams pool
# depends entirely on the workload's burstiness (#324: a fixed small pool
# SERIALISES under a burst — it does not error). For steady load, a larger
# static streams_pool_size is simpler and just as good.
#
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:pool_autoscale

require "logger"
require "concurrent"
require "pgbus"
require "pg"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost/pgbus_test bundle exec rake bench:pool_autoscale"
  exit 1
end

MIN_FREE_BACKENDS = 20
STREAM = "autoscale_#{Process.pid}".freeze

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def free_backends
  conn = PG.connect(DATABASE_URL)
  total = conn.exec("SHOW max_connections").first["max_connections"].to_i
  used  = conn.exec("SELECT count(*) AS n FROM pg_stat_activity").first["n"].to_i
  total - used
ensure
  conn&.close
end

def build_client
  config = Pgbus::Configuration.new.tap do |c|
    c.database_url = DATABASE_URL
    c.queue_prefix = "pgbus_autoscale"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.streams_pool_size = 3
    c.streams_pool_max = 10
    c.streams_pool_timeout = 2
    c.streams_pool_autoscale = true
    c.streams_pool_autoscale_interval = 0.1
  end
  Pgbus::Client.new(config, schema_ensured: true)
end

puts "=" * 76
puts "Streams-pool autoscaler benchmark (issue #323)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "=" * 76

if free_backends < MIN_FREE_BACKENDS
  warn "Only #{free_backends} Postgres backends free (need >= #{MIN_FREE_BACKENDS}); free some and retry."
  exit 1
end

client = build_client
client.ensure_stream_queue(STREAM)
autoscaler = Pgbus::Web::Streamer::PoolAutoscaler.new(client: client, config: client.config, logger: client.config.logger)

# --- (1) tick overhead: idle pool, measure the probe read + policy per tick ---
warmup = 5
warmup.times { autoscaler.tick }
iters = 200
t0 = monotonic
iters.times { autoscaler.tick }
per_tick_ms = ((monotonic - t0) / iters) * 1000.0

# --- (2) grow-under-load: churn the pool, tick, watch it grow ---
stop = Concurrent::AtomicBoolean.new(false)
churn = Array.new(8) do
  Thread.new do
    until stop.true?
      begin
        client.send(:streams_pool).with_connection { |c| c.exec("SELECT pg_sleep(0.03)") }
      rescue StandardError
        # pool timeout under contention is expected
      end
    end
  end
end

sizes = []
t_grow = monotonic
120.times do
  autoscaler.tick
  sizes << client.streams_pool_stats[:size]
  break if client.streams_pool_stats[:size] >= 10

  sleep 0.11 # let a sustained window build (real time, real cooldown)
end
grow_wall = monotonic - t_grow
stop.make_true
churn.each { |t| t.join(5) }

final = client.streams_pool_stats[:size]
swaps = client.streams_swap_stats.swap_count
autoscaler.stop
client.close

puts
puts "Tick overhead (idle):"
puts format("  %.3f ms/tick (headroom probe read + decision)", per_tick_ms)
puts "  -> at a 1s interval this is #{format("%.4f", per_tick_ms / 1000.0 * 100)}%% of a core; negligible."
puts
puts "Grow-under-load (3 -> cap 10, churn on 8 threads):"
puts "  size trajectory : #{sizes.first(12).inspect}#{" ..." if sizes.size > 12}"
puts "  final size      : #{final} (started 3)"
puts "  swaps           : #{swaps}"
puts format("  wall to settle  : %.1f s (bounded by the 15s cooldown between grows)", grow_wall)
puts
if final > 3
  puts "RESULT: the autoscaler reacted — grew the streams pool into headroom under a"
  puts "sustained DB-bound burst, capped at streams_pool_max. Per-op throughput is a"
  puts "workload question (#324): a fixed small pool serialises the same burst; a"
  puts "larger static pool avoids both. Enable autoscale for BURSTY streams load."
else
  puts "RESULT: no growth (load did not sustain saturation). For steady load, size"
  puts "streams_pool_size statically — autoscale earns its keep only under bursts."
end
puts "Done."
