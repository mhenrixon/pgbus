# frozen_string_literal: true

# Fair share read benchmark (issue #426) — real PostgreSQL.
#
#   PGBUS_DATABASE_URL=postgres://localhost:5432/pgbus_test bundle exec rake bench:fair_read
#
# Compares Client#read_batch (pgmq.read) against Client#read_batch_fair on
# three backlog shapes, and prints EXPLAIN (ANALYZE, BUFFERS) for the fair read
# on each so the cost model documented on Client::FairRead can be checked:
#
#   a) 100k visible messages, 1 key        — lone tenant, work-conserving path
#   b) 100k visible messages, 200 keys     — many contending tenants
#   c) 10k visible + 50k invisible, 50 keys — big in-flight / delayed / backoff set
#
# Every timed read puts the claimed messages back (vt = now) so the backlog
# stays constant across iterations; the reset is identical for both variants.
# Sizes are tunable via FAIR_BENCH_SCALE (default 1.0).

require "benchmark/ips"
require "json"
require "uri"
require "active_record"
require "pgbus"
require_relative "bench_support"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://localhost:5432/pgbus_test"
end
SCALE = ENV.fetch("FAIR_BENCH_SCALE", "1.0").to_f
QTY = ENV.fetch("FAIR_BENCH_QTY", "10").to_i

parsed_url = URI.parse(DATABASE_URL)
params = URI.decode_www_form(parsed_url.query || "").to_h
params["pool"] = "5"
parsed_url.query = URI.encode_www_form(params)
ActiveRecord::Base.establish_connection(parsed_url.to_s)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_fbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = false
  c.stats_enabled = false
end

QUEUE = "fair"
FULL = Pgbus.configuration.queue_name(QUEUE)
TABLE = "pgmq.q_#{FULL}".freeze
client = Pgbus.client
conn = ActiveRecord::Base.connection

def populate(client, conn, visible:, invisible:, keys:)
  client.purge_queue(QUEUE)
  batch = 5_000
  [[visible, 0], [invisible, 3_600]].each do |count, delay|
    next if count.zero?

    sent = 0
    while sent < count
      n = [batch, count - sent].min
      payloads = Array.new(n) do |i|
        k = (sent + i) % keys
        p = { "n" => sent + i, "pgbus_fair_key" => "tenant-#{k}" }
        p["pgbus_fair_weight"] = 3 if (k % 10).zero? && keys > 1
        p
      end
      client.send_batch(QUEUE, payloads, delay: delay)
      sent += n
    end
  end
  conn.execute("VACUUM ANALYZE #{TABLE}")
end

def reset_claimed(conn, messages)
  return if messages.empty?

  ids = messages.map { |m| m.msg_id.to_i }.join(",")
  conn.execute("UPDATE #{TABLE} SET vt = clock_timestamp() - interval '1 second' WHERE msg_id IN (#{ids})")
end

def explain_fair(client, conn)
  sql = client.send(:fair_read_sql, FULL)
  plan = conn.select_values("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) #{sql.gsub("$1", QTY.to_s).gsub("$2", "30")}")
  # EXPLAIN ANALYZE executes the UPDATE — put the claimed rows back.
  conn.execute(<<~SQL)
    UPDATE #{TABLE} SET vt = clock_timestamp() - interval '1 second'
     WHERE vt > clock_timestamp() + interval '20 seconds' AND vt < clock_timestamp() + interval '40 seconds'
  SQL
  plan
end

client.ensure_queue(QUEUE)
client.ensure_fair_index(QUEUE)

shapes = [
  { label: "a) 100k visible / 1 key", visible: (100_000 * SCALE).to_i, invisible: 0, keys: 1 },
  { label: "b) 100k visible / 200 keys", visible: (100_000 * SCALE).to_i, invisible: 0, keys: 200 },
  { label: "c) 10k visible + 50k invisible / 50 keys", visible: (10_000 * SCALE).to_i, invisible: (50_000 * SCALE).to_i, keys: 50 }
]

shapes.each do |shape|
  BenchSupport.header("#{shape[:label]}  (qty=#{QTY})")
  populate(client, conn, visible: shape[:visible], invisible: shape[:invisible], keys: shape[:keys])

  BenchSupport.ips(time: 3, warmup: 1) do |x|
    x.report("read_batch (pgmq.read)") do
      msgs = client.read_batch(QUEUE, qty: QTY, vt: 30)
      reset_claimed(conn, msgs)
    end
    x.report("read_batch_fair") do
      msgs = client.read_batch_fair(QUEUE, qty: QTY, vt: 30)
      reset_claimed(conn, msgs)
    end
  end

  puts "\nEXPLAIN (ANALYZE, BUFFERS) read_batch_fair:"
  explain_fair(client, conn).each { |line| puts "  #{line}" }
end

client.purge_queue(QUEUE)
