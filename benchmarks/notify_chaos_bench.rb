# frozen_string_literal: true

# Failure-mode measurements for the supervisor-owned LISTEN hub (issue #381
# step 12). Each scenario prints a number for the PR body; run on the same
# machine as notify_wake_bench.rb.
#
#   A. Killed LISTEN backend: time from pg_terminate_backend to the hub
#      observing degraded (healthy? false), and to full recovery (healthy?
#      true with a NEW backend + wakes flowing again).
#   B. Wedged fork: one registered fork stops draining its pipe (buffer
#      filled); the OTHER fork's wake latency must be unaffected.
#   C. FD hygiene: 50 register/deregister cycles must not grow the process's
#      open-FD count.
#   D. Fan-out cost: hub route() per-call cost with 10 registered forks —
#      the supervisor-side price of one NOTIFY. (A NOTIFY flood is bounded
#      upstream by the 250ms/queue trigger throttle, so per-call cost times
#      4/s/queue is the honest ceiling of hub load.)
#
# Supervisor-SIGKILL orphan behavior (pipe EOF → forks degrade to polling)
# is pinned at the unit level in spec/pgbus/process/wake_pipe_spec.rb — a
# full process-tree chaos run adds no information the EOF path doesn't.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:notify_chaos

require "benchmark"
require "logger"
require "active_record"
require "pgbus"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

ActiveRecord::Base.establish_connection(DATABASE_URL)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_cbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = true
  c.stats_enabled = false if c.respond_to?(:stats_enabled=)
  c.workers = [{ queues: %w[default], threads: 1 }]
end

client = Pgbus.client
client.ensure_queue("default")
client.purge_queue("default")

PHYSICAL_QUEUE = Pgbus.configuration.queue_name("default")

def monotonic_ms
  Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000.0
end

def drain(reader)
  reader.read_nonblock(4096)
rescue IO::WaitReadable
  ""
end

def wait_for_wake(reader, timeout_ms: 10_000)
  deadline = monotonic_ms + timeout_ms
  buffer = +""
  while monotonic_ms < deadline
    buffer << drain(reader)
    return monotonic_ms if buffer.include?("W")

    sleep 0.005
  end
  nil
end

def listen_backend_pids
  ActiveRecord::Base.connection.select_values(<<~SQL)
    SELECT pid FROM pg_stat_activity
    WHERE application_name = 'pgbus-listen' AND datname = current_database()
  SQL
end

def open_fd_count
  Dir.children("/dev/fd").size
rescue StandardError
  Dir.children("/proc/self/fd").size
end

puts "=" * 70
puts "Pgbus NotifyHub failure-mode measurements (issue #381)"
puts "=" * 70

hub = Pgbus::Process::NotifyHub.new(config: Pgbus.configuration, logger: Logger.new(IO::NULL))
hub.start
sleep 0.1 until hub.healthy?

reader_a, writer_a = IO.pipe
reader_b, writer_b = IO.pipe
hub.register_fork(pid: 111, queues: [PHYSICAL_QUEUE], pipe: writer_a)
hub.register_fork(pid: 222, queues: [PHYSICAL_QUEUE], pipe: writer_b)
sleep 0.5
drain(reader_a)
drain(reader_b)

# ─── A. Killed LISTEN backend ───
puts "\n--- A. pg_terminate_backend on the shared LISTEN connection ---"
old_pids = listen_backend_pids
t_kill = monotonic_ms
ActiveRecord::Base.connection.execute(<<~SQL)
  SELECT pg_terminate_backend(pid) FROM pg_stat_activity
  WHERE application_name = 'pgbus-listen' AND datname = current_database()
SQL

degraded_at = nil
recovered_at = nil
deadline = monotonic_ms + 30_000
while monotonic_ms < deadline
  hub.tick
  degraded_at ||= monotonic_ms unless hub.healthy?
  new_pids = listen_backend_pids
  if !new_pids.empty? && !new_pids.intersect?(old_pids) && hub.healthy?
    recovered_at = monotonic_ms
    break
  end
  sleep 0.01
end

if degraded_at
  puts format("  degraded observed after: %.0fms", degraded_at - t_kill)
else
  puts "  degraded window too short to observe (reconnect outran the 10ms poll) — expected on local PG"
end
puts format("  fresh backend + healthy after: %.0fms", recovered_at - t_kill) if recovered_at
abort "  RECOVERY FAILED within 30s" unless recovered_at

sleep 0.3
drain(reader_a)
t0 = monotonic_ms
client.send_message("default", { "post_kill" => true })
t_wake = wait_for_wake(reader_a)
abort "  post-recovery wake never arrived" unless t_wake
puts format("  first post-recovery wake latency: %.2fms", t_wake - t0)

# ─── B. Wedged fork ───
puts "\n--- B. wedged fork (pipe buffer full) does not affect siblings ---"
begin
  loop { writer_a.write_nonblock("x" * 4096) }
rescue IO::WaitWritable
  # fork A's pipe is now full — it stopped draining
end

samples = []
10.times do |i|
  drain(reader_b)
  sleep 0.3 # NOTIFY throttle window
  t0 = monotonic_ms
  client.send_message("default", { "wedge" => i })
  t_wake = wait_for_wake(reader_b)
  abort "  sibling wake never arrived while a fork was wedged" unless t_wake
  samples << (t_wake - t0)
end
puts format("  sibling wake latency while wedged: p50=%.2fms max=%.2fms (n=%d)",
            samples.sort[samples.size / 2], samples.max, samples.size)

# ─── C. FD hygiene over churn ───
puts "\n--- C. FD count across 50 register/deregister cycles ---"
before_fds = open_fd_count
50.times do |i|
  r, w = IO.pipe
  hub.register_fork(pid: 10_000 + i, queues: [PHYSICAL_QUEUE], pipe: w)
  hub.deregister_fork(10_000 + i)
  r.close
end
after_fds = open_fd_count
puts format("  open FDs before=%d after=%d (delta %+d)", before_fds, after_fds, after_fds - before_fds)
abort "  FD LEAK detected" if after_fds > before_fds

# ─── D. Fan-out cost ───
puts "\n--- D. hub route() per-call cost with 10 registered forks ---"
extra = Array.new(10) do |i|
  r, w = IO.pipe
  hub.register_fork(pid: 20_000 + i, queues: [PHYSICAL_QUEUE], pipe: w)
  [r, w]
end
channel = "#{Pgbus::Process::NotifyListener::CHANNEL_PREFIX}#{PHYSICAL_QUEUE}" \
          "#{Pgbus::Process::NotifyListener::CHANNEL_SUFFIX}"
elapsed = Benchmark.realtime do
  100_000.times { hub.send(:route, channel) }
end
puts format("  %.2fµs per NOTIFY routed to 10 forks (%.0f routes/s possible; " \
            "trigger throttle caps real load at 4/s/queue)",
            (elapsed / 100_000) * 1_000_000, 100_000 / elapsed)
extra.each do |r, w|
  r.close
  w.close unless w.closed?
end

hub.stop
[reader_a, reader_b, writer_a, writer_b].each { |io| io.close unless io.closed? }
client.purge_queue("default")
puts "\nDone."
