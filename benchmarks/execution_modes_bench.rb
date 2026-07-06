# frozen_string_literal: true

# Execution-mode connection benchmark (issue #323).
#
# Answers, with fair measurement, "for the same offered concurrency, how many
# real Postgres connections does threads mode vs async mode actually hold, and
# what throughput/latency does each deliver?" — so we can guide users on DB-pool
# sizing per execution_mode.
#
# THE HONEST FINDING it exists to prove/disprove: pgbus holds a pgmq connection
# only for the read/archive SQL round-trip, NOT the job body, so async does not
# "save connections" by yielding mid-checkout. Async's connection advantage is
# only the fraction of wall time spent OUTSIDE with_connection (io_light). For
# DB-bound work async is connection-bound like threads and a tiny pool STARVES.
# See ExecutionModeHarness for the mechanism.
#
# Requires PGBUS_DATABASE_URL. Run-and-report — never a CI gate.
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:execution_modes

require "logger"
require "concurrent"
require "pgbus"
require "pg"

require_relative "support/execution_mode_harness"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost:5432/pgbus_test bundle exec rake bench:execution_modes"
  exit 1
end

JOB_COUNT   = Integer(ENV.fetch("EM_BENCH_JOBS", "240"))
CONCURRENCY = Integer(ENV.fetch("EM_BENCH_CONCURRENCY", "12"))
MIN_FREE_BACKENDS = 20

IO_LIGHT = ExecutionModeHarness::IoProfile.io_light
DB_BOUND = ExecutionModeHarness::IoProfile.db_bound

# mode, pool_size, io_profile — concurrency + job_count are fixed so offered
# load is identical by construction across every row.
MATRIX = [
  [:threads, 12, IO_LIGHT], # 1 baseline: ~1 conn per busy thread's checkout window
  [:async,   12, IO_LIGHT], # 2 async, generously provisioned
  [:async,    3, IO_LIGHT], # 3 THE claim: can 3 conns serve 12 fibers? (io-light)
  [:threads,  3, IO_LIGHT], # 4 under-provisioned threads -> expect pool_timeouts
  [:threads, 12, DB_BOUND], # 5 disproof baseline
  [:async,   12, DB_BOUND], # 6 disproof: async peak_busy should ~= threads
  [:async,    3, DB_BOUND], # 7 honesty: does 3 starve when DB-bound?
  [:async,    6, IO_LIGHT]  # 8 the interesting middle
].freeze

def build_client(pool_size)
  config = Pgbus::Configuration.new.tap do |c|
    c.database_url = DATABASE_URL
    c.queue_prefix = "pgbus_execmode"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pool_size = pool_size
    c.pool_timeout = 5
    c.stats_enabled = false
  end
  Pgbus::Client.new(config, schema_ensured: true)
end

def free_backends
  conn = PG.connect(DATABASE_URL)
  total = conn.exec("SHOW max_connections").first["max_connections"].to_i
  used  = conn.exec("SELECT count(*) AS n FROM pg_stat_activity").first["n"].to_i
  total - used
ensure
  conn&.close # close even if a query raises (preflight! rescues the error)
end

def preflight!
  free = free_backends
  return if free >= MIN_FREE_BACKENDS

  warn "Only #{free} Postgres backends free (need >= #{MIN_FREE_BACKENDS}). " \
       "The local box is shared; free some connections and retry."
  exit 1
rescue PG::Error => e
  warn "Preflight connection failed: #{e.message}"
  exit 1
end

def run_row(mode, pool_size, io_profile)
  return skip_async_if_missing(mode) if mode == :async && !async_available?

  client = build_client(pool_size)
  begin
    ExecutionModeHarness.run_cell(
      mode: mode, pool_size: pool_size, concurrency: CONCURRENCY,
      io_profile: io_profile, job_count: JOB_COUNT, client: client
    )
  ensure
    client.close
  end
rescue PG::ConnectionBad, PGMQ::Errors::ConnectionError => e
  warn "  skipped #{mode}/pool=#{pool_size}/#{io_profile.label}: #{e.message.lines.first&.strip}"
  nil
end

def async_available?
  require "async"
  true
rescue LoadError
  false
end

def skip_async_if_missing(_mode)
  warn "  skipped async row: the `async` gem is not installed (add gem \"async\")"
  nil
end

def print_table(results)
  headers = ExecutionModeHarness::Result.headers
  widths = headers.map.with_index do |h, i|
    [h.length, *results.map { |r| r.to_row[i].to_s.length }].max
  end
  puts headers.each_with_index.map { |h, i| h.ljust(widths[i]) }.join("  ")
  puts widths.map { |w| "-" * w }.join("  ")
  results.each do |r|
    puts r.to_row.each_with_index.map { |v, i| v.to_s.ljust(widths[i]) }.join("  ")
  end
end

puts "=" * 78
puts "Execution-mode connection benchmark (issue #323)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "Offered load per row: #{JOB_COUNT} jobs at concurrency #{CONCURRENCY} (identical for every row)"
puts "=" * 78

preflight!

results = []
MATRIX.each do |mode, pool_size, io_profile|
  print "running #{mode}/pool=#{pool_size}/#{io_profile.label} ... "
  $stdout.flush
  result = run_row(mode, pool_size, io_profile)
  puts result ? "done" : "skipped"
  results << result if result
  sleep 1 # let connections drain between cells (shared box)
end

puts
print_table(results)

puts
puts "Reading this table:"
puts "  peak_busy = max(size - available) = live pool checkouts. This is the"
puts "  connection cost of the offered load — NOT a throughput number."
puts "  pool_timeouts > 0 means the pool was too small for the offered concurrency."
puts
puts "Honest framing:"
puts "  * A job holds a pgmq connection only for the read/archive round-trip, not"
puts "    the job body. Async does NOT save connections by yielding mid-checkout."
puts "  * io_light (work mostly OUTSIDE the checkout): async can share a tiny pool."
puts "  * db_bound (work mostly INSIDE the checkout): async is connection-bound"
puts "    like threads; a tiny pool starves (pool_timeouts)."
puts "  * async DB concurrency is reactor-bound unless a fiber-aware PG driver is"
puts "    used — SELECT pg_sleep in with_connection is a blocking libpq call."
puts
puts "Done."
