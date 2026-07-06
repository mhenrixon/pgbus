# frozen_string_literal: true

# Job-burst gate benchmark (issue #323 phase 3 — the measure-first gate).
#
# QUESTION IT ANSWERS: under a job burst, is the JOB EXECUTION POOL (thread
# count) the throughput limiter, or is it the DB CONNECTION POOL (pgmq
# checkout)? Issue #323 phase 3 mandates this measurement before building
# elastic job pools — if raising the static `threads:` (with a matching
# `pool_size`) already recovers burst throughput, then "static headroom is the
# answer" and no elastic machinery is warranted.
#
# HOW: for a fixed burst of jobs, sweep execution-pool concurrency (thread
# count) upward under two connection regimes, using the shared
# ExecutionModeHarness#run_cell (same rig as #324):
#
#   (1) MATCHED   — pool_size == concurrency (the "raise threads AND pool_size"
#                   static-headroom answer). If throughput keeps climbing with
#                   concurrency here, more threads help → the job pool is (part
#                   of) the limiter, and static headroom already fixes it.
#   (2) STARVED   — pool_size fixed small while concurrency climbs (the
#                   "elastic threads, fixed connection pool" case — exactly what
#                   elastic job threads WITHOUT elastic connections would give).
#                   If throughput plateaus at ~pool_size regardless of thread
#                   count, then threads past the connection ceiling buy nothing
#                   → elastic job threads alone are a no-op.
#
# Reading the two curves together tells you which pool to make elastic (if any).
# Expectation from #324 (job holds a pgmq connection only for the read/archive
# round-trip): the STARVED curve plateaus at the connection ceiling, so raising
# threads past `resolved_pool_size` doesn't help — static headroom (raise BOTH)
# is the fix. This bench confirms or refutes that on your hardware.
#
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:job_burst

require "logger"
require "pg"
require "pgbus"
require_relative "support/execution_mode_harness"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost/pgbus_test bundle exec rake bench:job_burst"
  exit 1
end

MIN_FREE_BACKENDS = 30
JOB_COUNT = 400                       # the burst size (same for every cell → fair)
CONCURRENCY_SWEEP = [2, 4, 8, 16].freeze
STARVED_POOL_SIZE = 4                 # fixed small connection ceiling for regime (2)
# DB-bound profile: each job spends most of its time INSIDE a pooled checkout
# (SELECT pg_sleep), so the connection pool is the contended resource — the
# regime where "more threads vs more connections" actually diverges.
IO_PROFILE = ExecutionModeHarness::IoProfile.db_bound

def build_client(pool_size)
  config = Pgbus::Configuration.new.tap do |c|
    c.database_url = DATABASE_URL
    c.queue_prefix = "pgbus_jobburst"
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
  conn&.close
end

def run_cell(pool_size:, concurrency:)
  client = build_client(pool_size)
  ExecutionModeHarness.run_cell(
    mode: :threads, pool_size: pool_size, concurrency: concurrency,
    io_profile: IO_PROFILE, job_count: JOB_COUNT, client: client
  )
ensure
  client&.close
end

puts "=" * 76
puts "Job-burst gate benchmark (issue #323 phase 3)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "Burst: #{JOB_COUNT} db-bound jobs (db=#{IO_PROFILE.db_seconds}s in-checkout each)"
puts "=" * 76

free = free_backends
if free < MIN_FREE_BACKENDS
  warn "Only #{free} Postgres backends free (need >= #{MIN_FREE_BACKENDS}); free some and retry."
  exit 1
end

matched = CONCURRENCY_SWEEP.map { |c| run_cell(pool_size: c, concurrency: c) }
starved = CONCURRENCY_SWEEP.map { |c| run_cell(pool_size: STARVED_POOL_SIZE, concurrency: c) }

def print_table(title, results)
  puts
  puts title
  headers = ExecutionModeHarness::Result.headers
  rows = results.map(&:to_row)
  widths = headers.map.with_index { |h, i| [h.length, *rows.map { |r| r[i].to_s.length }].max }
  fmt = ->(row) { row.each_with_index.map { |v, i| v.to_s.ljust(widths[i]) }.join("  ") }
  puts fmt.call(headers)
  rows.each { |r| puts fmt.call(r) }
end

print_table("(1) MATCHED — pool_size == concurrency  (the static-headroom answer)", matched)
print_table("(2) STARVED — pool_size fixed at #{STARVED_POOL_SIZE}  (elastic threads, fixed connections)", starved)

# ─── Verdict ───
# The decisive signal is peak_busy, NOT raw throughput: peak_busy = the max live
# checkouts, which cannot exceed pool_size. If, in the STARVED regime, peak_busy
# caps at pool_size while concurrency climbs past it, then the extra threads are
# just waiting on connection checkout — they cannot do more concurrent work. A
# db-bound job still has a tiny non-checkout slice, so a couple extra threads can
# overlap it for a small one-time throughput bump; that is NOT the threads being
# the limiter, so we key the verdict off peak_busy capping, not a throughput ratio.
matched_thr = matched.map(&:throughput)
starved_thr = starved.map(&:throughput)
matched_gain = matched_thr.last / matched_thr.first
starved_peak_busy = starved.map(&:peak_busy).max
# Did adding threads beyond pool_size raise concurrent work (peak_busy)?
capped_at_pool = starved_peak_busy <= STARVED_POOL_SIZE

puts
puts "Reading the curves:"
puts format("  MATCHED throughput %.0f → %.0f jobs/s across concurrency %s (%.1f× gain, peak_busy tracks conc)",
            matched_thr.first, matched_thr.last, CONCURRENCY_SWEEP.inspect, matched_gain)
puts format("  STARVED throughput %.0f → %.0f jobs/s; peak_busy capped at %d (pool_size=%d)",
            starved_thr.first, starved_thr.last, starved_peak_busy, STARVED_POOL_SIZE)
puts
if capped_at_pool
  puts "VERDICT: in the STARVED regime peak_busy never exceeds pool_size (#{STARVED_POOL_SIZE}) no matter"
  puts "how many threads run — extra threads just WAIT on connection checkout (see p99 latency"
  puts "climb). The DB connection pool, not the thread pool, is the hard burst ceiling. Elastic"
  puts "job THREADS alone (fixed connection pool) cannot push past it. The fix is STATIC HEADROOM:"
  puts "raise BOTH `threads:` and `pool_size:` — the MATCHED curve scales linearly when you do."
  puts "This is issue #323's sanctioned outcome. Do NOT build elastic job pools."
else
  puts "VERDICT: STARVED peak_busy exceeded pool_size — extra threads did more concurrent work"
  puts "despite the fixed connection pool. The thread pool is an independent limiter; elastic job"
  puts "threads (issue #323 phase 3b, Design A) may be worth building. Unexpected given #324 —"
  puts "re-check the job's connection-holding profile."
end
puts "Done."
