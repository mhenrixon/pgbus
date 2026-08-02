# frozen_string_literal: true

# NOTIFY wake-path benchmark (issue #381) — measures the numbers the
# supervisor-owned LISTEN change must hold or improve:
#
#   1. Wake latency (direct NotifyListener): send_message → on_wake fires.
#      This is today's per-fork path and the before/after control.
#   2. Wake latency (hub-mediated): send_message → 'W' byte readable on a
#      registered fork wake pipe. Only runs when Pgbus::Process::NotifyHub is
#      defined (i.e. on the #381 branch); on main this section is skipped, so
#      the same file produces the baseline and the after numbers.
#   3. Empty-read cost: DB-side price of one blind read on an idle queue, plus
#      the projected per-fork read load at fast-poll (0.1s) vs the NOTIFY
#      ceiling (15s) — the load the listener exists to avoid.
#   4. Direct-LISTEN connection census: pg_stat_activity rows grouped by
#      application_name (DedicatedConnection stamps "pgbus-listen").
#
# Samples are spaced beyond Client::NOTIFY_THROTTLE_MS (250ms) apart — the pgmq
# trigger coalesces NOTIFYs per window, so back-to-back sends would measure
# the poll fallback, not the NOTIFY path.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:notify_wake

require "benchmark/ips"
require "json"
require "logger"
require "active_record"
require "pgbus"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

SAMPLES = Integer(ENV.fetch("WAKE_SAMPLES", "100"))
THROTTLE_GAP_S = 0.35 # > NOTIFY_THROTTLE_MS so every send fires a NOTIFY

ActiveRecord::Base.establish_connection(DATABASE_URL)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_wbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = true
  c.stats_enabled = false if c.respond_to?(:stats_enabled=)
end

client = Pgbus.client
client.ensure_queue("default")
client.purge_queue("default")

PHYSICAL_QUEUE = Pgbus.configuration.queue_name("default")

def percentile(sorted, pct)
  return nil if sorted.empty?

  sorted[[(sorted.size * pct / 100.0).ceil - 1, 0].max]
end

def report_latencies(label, samples_ms)
  sorted = samples_ms.sort
  puts format(
    "%-28<label>s n=%<n>d  p50=%<p50>.2fms  p95=%<p95>.2fms  p99=%<p99>.2fms  max=%<max>.2fms",
    label: label, n: sorted.size,
    p50: percentile(sorted, 50), p95: percentile(sorted, 95),
    p99: percentile(sorted, 99), max: sorted.last
  )
end

def monotonic_ms
  Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000.0
end

puts "=" * 70
puts "Pgbus NOTIFY wake-path benchmark (issue #381)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}  samples=#{SAMPLES}"
puts "=" * 70

# ═══════════════════════════════════════════════════════════════════════
# 1. Direct NotifyListener wake latency (today's per-fork path)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 1. Wake latency: direct NotifyListener ---"

woke = Queue.new
# ->(*) works on main (on_wake.call) and on the #381 branch (on_wake.call(channel)).
listener = Pgbus::Process::NotifyListener.new(
  physical_queues: [PHYSICAL_QUEUE],
  on_wake: ->(*) { woke << monotonic_ms },
  connection_options: Pgbus.configuration.worker_notify_connection_options,
  health_check_ms: 1000,
  logger: Logger.new(IO::NULL)
).start
sleep 1 # let LISTEN register

direct_samples = []
SAMPLES.times do |i|
  woke.clear
  t0 = monotonic_ms
  client.send_message("default", { "bench" => i })
  t_wake = woke.pop # blocks until on_wake
  direct_samples << (t_wake - t0)
  sleep THROTTLE_GAP_S
end
listener.stop
client.purge_queue("default")
report_latencies("direct listener", direct_samples)

# ═══════════════════════════════════════════════════════════════════════
# 2. Hub-mediated wake latency (only on the #381 branch)
# ═══════════════════════════════════════════════════════════════════════

if defined?(Pgbus::Process::NotifyHub)
  puts "\n--- 2. Wake latency: NotifyHub → fork pipe ---"

  reader, writer = IO.pipe
  hub = Pgbus::Process::NotifyHub.new(config: Pgbus.configuration, logger: Logger.new(IO::NULL))
  hub.start
  hub.register_fork(pid: 999_999, queues: [PHYSICAL_QUEUE], pipe: writer)
  sleep 1

  hub_samples = []
  SAMPLES.times do |i|
    t0 = monotonic_ms
    client.send_message("default", { "bench" => i })
    reader.wait_readable(10) or abort "hub wake never arrived"
    t_wake = monotonic_ms
    reader.read_nonblock(64) # drain W (+ any status bytes)
    hub_samples << (t_wake - t0)
    sleep THROTTLE_GAP_S
  end
  hub.deregister_fork(999_999)
  hub.stop
  reader.close
  client.purge_queue("default")
  report_latencies("hub → pipe", hub_samples)
else
  puts "\n--- 2. NotifyHub not defined (main baseline) — skipped ---"
end

# ═══════════════════════════════════════════════════════════════════════
# 3. Empty-read cost + projected idle load
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 3. Empty-read cost (idle queue) ---"

result = Benchmark.ips do |x|
  x.config(time: 5, warmup: 1)
  x.report("empty read_batch") { client.read_batch("default", qty: 5) }
end
ips = result.entries.first.ips
puts format("  one empty read: %.2fms DB round-trip (%.0f reads/s possible)", 1000.0 / ips, ips)
puts format("  projected idle load per fork: %.1f reads/s at 0.1s fast-poll vs %.3f reads/s at 15s NOTIFY ceiling",
            1.0 / 0.1, 1.0 / 15)

# ═══════════════════════════════════════════════════════════════════════
# 4. Direct-LISTEN connection census
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 4. Connection census (5 simulated fork listeners) ---"

listeners = Array.new(5) do
  Pgbus::Process::NotifyListener.new(
    physical_queues: [PHYSICAL_QUEUE],
    on_wake: ->(*) {},
    connection_options: Pgbus.configuration.worker_notify_connection_options,
    health_check_ms: 1000,
    logger: Logger.new(IO::NULL)
  ).start
end
sleep 1

census = ActiveRecord::Base.connection.select_rows(<<~SQL)
  SELECT application_name, count(*)
  FROM pg_stat_activity
  WHERE application_name LIKE 'pgbus%'
  GROUP BY application_name ORDER BY application_name
SQL
if census.empty?
  puts "  no pgbus-tagged backends visible (main baseline: DedicatedConnection sets no application_name)"
else
  census.each { |name, count| puts format("  %-24<name>s %<count>d", name: name, count: count) }
end
listeners.each(&:stop)

client.purge_queue("default")
puts "\nDone."
