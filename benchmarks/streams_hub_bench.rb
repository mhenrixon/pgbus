# frozen_string_literal: true

# Streams master-hub latency benchmark (issue #382): measures the price of
# the master→worker socket hop by running the SAME single-broadcast SSE
# roundtrip twice —
#
#   A. :process — the per-worker Listener path (pre-#382 architecture)
#   B. :master  — MasterHub in-process, the streamer on a FailoverListener
#      over the Unix socket (one extra frame hop per wake)
#
# plus the LISTEN-connection census for each mode. Compare column A against
# main's streams_bench section 1 to isolate refactor noise from hop cost.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:streams_hub

require "json"
require "logger"
require "tmpdir"
require "securerandom"
require "active_record"
require "pgbus"

require_relative "../spec/support/puma_test_harness"
require_relative "../spec/support/sse_test_client"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

SAMPLES = Integer(ENV.fetch("HUB_BENCH_SAMPLES", "50"))

ActiveRecord::Base.establish_connection(DATABASE_URL)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_hbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = true
  c.streams_signed_name_secret = "a" * 64
  c.streams_listen_health_check_ms = 100
  c.streams_heartbeat_interval = 30
  c.streams_write_deadline_ms = 5_000
  # Durable broadcasts: race-immune against subscription setup (a broadcast
  # landing before LISTEN is active is still caught by the connect-time
  # read_after) and the representative wake -> read_after -> fanout path.
  c.streams_default_broadcast_mode = :durable
  c.stats_enabled = false if c.respond_to?(:stats_enabled=)
end

def percentile(sorted, pct)
  sorted[[(sorted.size * pct / 100.0).ceil - 1, 0].max]
end

def census
  ActiveRecord::Base.connection.select_value(<<~SQL).to_i
    SELECT count(*) FROM pg_stat_activity
    WHERE application_name = 'pgbus-listen' AND datname = current_database()
  SQL
end

def measure_roundtrips(label)
  stream_name = "hb_#{SecureRandom.hex(4)}"
  Pgbus.client.ensure_stream_queue(stream_name)
  streamer = Pgbus::Web::Streamer::Instance.new(
    client: Pgbus.client, config: Pgbus.configuration, logger: Logger.new(IO::NULL)
  )
  streamer.start
  app = Pgbus::Web::StreamApp.new(
    streamer: streamer, config: Pgbus.configuration, logger: Logger.new(IO::NULL)
  )
  harness = SseTestSupport::PumaTestHarness.boot(rack_app: app)
  stream = Pgbus.stream(stream_name)
  signed = Pgbus::Streams::SignedName.sign(stream_name)
  client = SseTestSupport::SseTestClient.connect(
    url: "#{harness.url("/#{signed}")}?since=#{stream.current_msg_id}", timeout: 5
  )

  listener_kind = streamer.listener.class.name.split("::").last
  mode_census = census
  # Warmup: proves the subscription is live before timing starts.
  stream.broadcast("<turbo-stream>warmup</turbo-stream>")
  abort "#{label}: warmup broadcast never delivered" if
    client.wait_for_events(count: 1, timeout: 10).empty?

  samples = []
  SAMPLES.times do |i|
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    stream.broadcast("<turbo-stream>#{i}</turbo-stream>")
    events = client.wait_for_events(count: i + 2, timeout: 10)
    # A silently dropped/late wake would otherwise record a ~10s sample
    # straight into the reported percentiles.
    abort "#{label}: sample #{i} never delivered (got #{events.size}, expected #{i + 2})" if events.size < i + 2
    samples << ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0)
  end

  sorted = samples.sort
  puts format(
    "%-32<label>s listener=%-16<kind>s census=%<census>d  n=%<n>d  " \
    "p50=%<p50>.2fms  p95=%<p95>.2fms  max=%<max>.2fms",
    label: label, kind: listener_kind, census: mode_census, n: sorted.size,
    p50: percentile(sorted, 50), p95: percentile(sorted, 95), max: sorted.last
  )
ensure
  client&.close
  streamer&.shutdown!
  harness&.shutdown
end

puts "=" * 70
puts "Pgbus streams master-hub benchmark (issue #382)  samples=#{SAMPLES}"
puts "=" * 70

# ─── A. :process (per-worker listener — the pre-#382 path) ───
Pgbus.configuration.streams_listen_scope = :process
measure_roundtrips("A. :process (per-worker)")

# ─── B. :master (hub interposed) ───
tmpdir = Dir.mktmpdir("pgbus-hub-bench")
socket_path = File.join(tmpdir, "hub.sock")
hub = Pgbus::Web::Streamer::MasterHub.new(
  config: Pgbus.configuration, socket_path: socket_path, logger: Logger.new(IO::NULL)
)
hub.start
ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path
Pgbus.configuration.streams_listen_scope = :master
begin
  measure_roundtrips("B. :master (hub -> socket hop)")
ensure
  ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
  hub.stop
  FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
end

puts "\nDone."
