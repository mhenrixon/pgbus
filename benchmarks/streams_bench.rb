# frozen_string_literal: true

# Streams benchmark — measures real fanout throughput, connect latency,
# and broadcast-to-delivery p99 against a real Puma server, real PGMQ,
# and real LISTEN/NOTIFY.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:streams
#
# What it measures:
#
#   1. Single-broadcast roundtrip: time from Stream#broadcast to the
#      SSE client receiving the message. This is the per-event latency
#      that wake-coalescing and fanout-batching changes affect directly.
#
#   2. Burst broadcast throughput: time to deliver N broadcasts to one
#      connected SSE client. This is where wake coalescing earns its
#      keep — without coalescing, N broadcasts = N read_after queries.
#
#   3. Fanout to many connections: time to deliver one broadcast to M
#      simultaneously connected SSE clients. This is where fanout
#      batching matters — Connection#enqueue called M times means M
#      io.write_nonblock calls.
#
#   4. Connect latency under thundering herd: time to open K SSE
#      connections concurrently. This is where Listener mutex
#      contention shows up if ensure_listening serializes badly.
#
# Numbers from this file are committed inline to PR #88 (perf sweep).
# Re-running on a different machine produces different absolute numbers
# but the BEFORE/AFTER ratios should hold across hardware.

require "benchmark/ips"
require "json"
require "uri"
require "securerandom"
require "logger"
require "active_record"
require "pgbus"

require_relative "../spec/support/puma_test_harness"
require_relative "../spec/support/sse_test_client"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

parsed_url = URI.parse(DATABASE_URL)
params = URI.decode_www_form(parsed_url.query || "").to_h
params["pool"] = "20"
parsed_url.query = URI.encode_www_form(params)
ActiveRecord::Base.establish_connection(parsed_url.to_s)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_sbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = true
  c.stats_enabled = false
  c.streams_signed_name_secret = "a" * 64
  c.streams_listen_health_check_ms = 100
  c.streams_heartbeat_interval = 60
  c.streams_write_deadline_ms = 5_000
end

def build_pg_listen_connection
  require "pg"
  uri = URI.parse(DATABASE_URL)
  PG.connect(
    host: uri.host || "localhost",
    port: (uri.port || 5432).to_i,
    dbname: uri.path.delete_prefix("/"),
    user: uri.user || ENV.fetch("USER"),
    password: uri.password
  )
end

def boot_streamer
  streamer = Pgbus::Web::Streamer::Instance.new(
    client: Pgbus.client,
    config: Pgbus.configuration,
    pg_connection: build_pg_listen_connection,
    logger: Logger.new(IO::NULL)
  )
  streamer.start
  streamer
end

def boot_harness(streamer)
  app = Pgbus::Web::StreamApp.new(
    streamer: streamer,
    config: Pgbus.configuration,
    logger: Logger.new(IO::NULL)
  )
  SseTestSupport::PumaTestHarness.boot(rack_app: app)
end

def signed(name)
  Pgbus::Streams::SignedName.sign(name)
end

def with_clean_stream
  stream_name = "sb_#{SecureRandom.hex(4)}"
  Pgbus.client.ensure_stream_queue(stream_name)
  streamer = boot_streamer
  harness = boot_harness(streamer)
  yield(stream_name, streamer, harness)
ensure
  streamer&.shutdown!
  harness&.shutdown
end

def connect_client(harness, stream_name, since_id: 0)
  url = "#{harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
  SseTestSupport::SseTestClient.connect(url: url, timeout: 5)
end

puts "=" * 70
puts "Pgbus Streams Benchmarks (real Puma + real PGMQ + real SSE)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}"
puts "=" * 70

# ═══════════════════════════════════════════════════════════════════════
# 1. Single broadcast roundtrip — time from broadcast to client receipt
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 1. Single broadcast roundtrip latency ---"
puts "    (broadcast → NOTIFY → Listener → Dispatcher → Connection → SseTestClient)"

with_clean_stream do |stream_name, _streamer, harness|
  stream = Pgbus.stream(stream_name)
  client = connect_client(harness, stream_name, since_id: stream.current_msg_id)
  sleep 0.1 # let the connect settle into the registry

  # Warm up so we're measuring steady-state, not first-NOTIFY-after-quiet
  3.times do
    stream.broadcast("<turbo-stream>warmup</turbo-stream>")
    client.wait_for_events(count: client.events.size + 1, timeout: 2)
  end

  samples = []
  50.times do
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    target = client.events.size + 1
    stream.broadcast("<turbo-stream>x</turbo-stream>")
    client.wait_for_events(count: target, timeout: 2)
    samples << ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0)
  end

  samples.sort!
  printf "    n=%d  min=%6.2fms  median=%6.2fms  p95=%6.2fms  max=%6.2fms\n",
         samples.size, samples.min, samples[samples.size / 2], samples[(samples.size * 0.95).to_i], samples.max

  client.close
end

# ═══════════════════════════════════════════════════════════════════════
# 2. Burst throughput — N broadcasts to one client, total time
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 2. Burst broadcast throughput (N broadcasts → 1 client) ---"
puts "    (this is where wake coalescing matters — N notifies = N read_after calls without it)"

[10, 50, 200].each do |burst_size|
  with_clean_stream do |stream_name, _streamer, harness|
    stream = Pgbus.stream(stream_name)
    client = connect_client(harness, stream_name, since_id: stream.current_msg_id)
    sleep 0.1

    # Warm: send one broadcast first so the steady-state path is hot.
    stream.broadcast("<turbo-stream>warm</turbo-stream>")
    client.wait_for_events(count: 1, timeout: 5)
    warm_baseline = client.events.size

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    burst_size.times { |i| stream.broadcast("<turbo-stream>burst-#{i}</turbo-stream>") }
    client.wait_for_events(count: warm_baseline + burst_size, timeout: 30)
    elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

    received = client.events.size - warm_baseline
    printf "    burst=%3d  delivered=%3d  total=%7.2fms  per-msg=%5.2fms\n",
           burst_size, received, elapsed, elapsed / burst_size

    client.close
  end
end

# ═══════════════════════════════════════════════════════════════════════
# 3. Fanout — one broadcast to M simultaneously connected clients
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 3. Fanout (1 broadcast → M clients) ---"
puts "    (this is where Connection#enqueue batching matters — M connections = M io.write calls)"

[5, 20, 50].each do |fanout|
  with_clean_stream do |stream_name, _streamer, harness|
    stream = Pgbus.stream(stream_name)
    clients = Array.new(fanout) { connect_client(harness, stream_name, since_id: stream.current_msg_id) }
    sleep 0.5 # let all connects settle into the registry

    # Warm: do one broadcast to clear cold-start effects, then measure
    # the SECOND broadcast.
    stream.broadcast("<turbo-stream>warm</turbo-stream>")
    deadline = Time.now + 10
    sleep 0.005 until clients.all? { |c| c.events.size >= 1 } || Time.now > deadline
    warm_baseline = clients.map { |c| c.events.size }

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    stream.broadcast("<turbo-stream>fanout</turbo-stream>")
    deadline = Time.now + 5
    target = warm_baseline.first + 1
    sleep 0.005 until clients.all? { |c| c.events.size >= target } || Time.now > deadline
    elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

    delivered = clients.count { |c| c.events.size >= target }
    printf "    M=%2d  delivered=%2d  total=%7.2fms  per-client=%5.2fms\n",
           fanout, delivered, elapsed, elapsed / fanout

    clients.each(&:close)
  end
end

# ═══════════════════════════════════════════════════════════════════════
# 4. Concurrent connect latency — K simultaneous SSE handshakes
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- 4. Concurrent connect latency (K clients connecting at once) ---"
puts "    (this is where Listener mutex contention matters — K connects = K LISTEN waits)"

[5, 20, 50].each do |k|
  with_clean_stream do |stream_name, _streamer, harness|
    stream = Pgbus.stream(stream_name)
    since = stream.current_msg_id

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    threads = Array.new(k) do
      Thread.new { connect_client(harness, stream_name, since_id: since) }
    end
    clients = threads.map(&:value)
    elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

    printf "    K=%2d  total=%7.2fms  per-connect=%5.2fms\n", k, elapsed, elapsed / k

    clients.each(&:close)
  end
end

puts "\nDone."
