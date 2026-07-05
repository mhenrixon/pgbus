# frozen_string_literal: true

# Streams read-pool benchmark (issue #315).
#
# Measures the per-wake latency of the durable-stream replay read
# (Client#read_after), which the dispatcher runs on every NOTIFY wake and
# every SSE connect. Before #315 this went through with_raw_connection, which
# on the String/Hash config does a full PG.connect + close PER CALL (TCP +
# auth + TLS). After #315 it checks out a persistent connection from a
# dedicated streams pool.
#
# This is a same-machine, same-query A/B: the "fresh-connect-per-call" arm
# reproduces the OLD behavior (PG.connect per read) and the "pooled" arm runs
# the NEW behavior (Client#read_after over the streams pool). The delta is the
# per-wake connection-setup cost the pool eliminates.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:one[streams_read_pool_bench]
#
# NOTE: this measures the CONNECTION-SETUP win at the client layer, not
# end-to-end broadcast-to-browser latency (which is dominated by the PGMQ
# round-trip + LISTEN/NOTIFY + socket write). See benchmarks/streams_bench.rb
# for the full-stack numbers.

require "benchmark/ips"
require "securerandom"
require "logger"
require "concurrent"
require "pg"
require "pgbus"

require_relative "bench_support"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

STREAM_NAME = "readpoolbench_#{SecureRandom.hex(4)}".freeze
SEED_ROWS = Integer(ENV.fetch("SEED_ROWS", "50"))

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_rpbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.stats_enabled = false
  c.streams_pool_size = 5
end

client = Pgbus.client

# Seed a stream queue with SEED_ROWS durable broadcasts so read_after has real
# rows to union across the live (q_) and archive (a_) tables.
SEED_ROWS.times do |i|
  client.send_stream_message(STREAM_NAME, { "html" => "<turbo-stream>#{i}</turbo-stream>" })
end

sanitized = Pgbus::QueueNameValidator.sanitize!(client.config.queue_name(STREAM_NAME))
READ_SQL = <<~SQL.freeze
  (
    SELECT msg_id, enqueued_at, message, 'live'::text AS source
    FROM pgmq.q_#{sanitized}
    WHERE msg_id > $1 ORDER BY msg_id ASC LIMIT $2
  )
  UNION ALL
  (
    SELECT msg_id, enqueued_at, message, 'archive'::text AS source
    FROM pgmq.a_#{sanitized}
    WHERE msg_id > $1 ORDER BY msg_id ASC LIMIT $2
  )
  ORDER BY msg_id ASC LIMIT $2
SQL

puts "=" * 70
puts "Streams read-pool benchmark (issue #315)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "Stream:   #{STREAM_NAME} (#{SEED_ROWS} seeded rows)"
puts "=" * 70

BenchSupport.header("read_after latency: fresh PG.connect per call (OLD) vs pooled (NEW)")

BenchSupport.ips(time: 5, warmup: 2) do |x|
  # OLD behavior: a brand-new PG.connect + close on every read, exactly what
  # with_raw_connection did on the String/Hash path before #315.
  x.report("fresh-connect-per-call") do
    conn = PG.connect(DATABASE_URL)
    conn.exec_params(READ_SQL, [0, 500]).to_a
  ensure
    conn&.close
  end

  # NEW behavior: Client#read_after over the dedicated streams pool.
  x.report("pooled read_after") do
    client.read_after(STREAM_NAME, after_id: 0, limit: 500)
  end
end

BenchSupport.header("allocations per read_after (pooled path)")
BenchSupport.allocations("pooled read_after") do
  100.times { client.read_after(STREAM_NAME, after_id: 0, limit: 500) }
end

# Clean up the throwaway benchmark queue.
begin
  client.drop_queue(client.config.queue_name(STREAM_NAME), prefixed: false)
rescue StandardError => e
  warn "cleanup failed (harmless): #{e.class}: #{e.message}"
end

puts "\nInterpretation: the gap between the two bars is the per-wake connection"
puts "setup cost (TCP + auth + TLS) that the dedicated streams pool removes."
puts "Done."
