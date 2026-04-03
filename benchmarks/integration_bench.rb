# frozen_string_literal: true

# Integration benchmark — measures real PostgreSQL performance.
# Requires PGBUS_DATABASE_URL to be set:
#   PGBUS_DATABASE_URL=postgres://pgbus:pgbus@localhost:5432/pgbus_test bin/rake bench:integration

require "benchmark/ips"
require "memory_profiler"
require "json"
require "securerandom"
require "active_record"
require "active_job"
require "pgbus"

ActiveJob::Base.logger = Logger.new(IO::NULL)

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://pgbus:pgbus@localhost:5432/pgbus_test"
end

ActiveRecord::Base.establish_connection("#{DATABASE_URL}?pool=20")

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_ibench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = false
  c.stats_enabled = false
end

# Ensure schema + queues exist
Pgbus.client.ensure_queue("default")
Pgbus.client.ensure_dead_letter_queue("default")

# Ensure pgbus tables exist
conn = ActiveRecord::Base.connection
%w[pgbus_job_locks pgbus_semaphores pgbus_failed_events].each do |table|
  next if conn.table_exists?(table)

  abort "Table #{table} does not exist. Run the pgbus migrations first."
end

# ─── Job classes ───
# rubocop:disable Style/OneClassPerFile

class PlainBenchJob < ActiveJob::Base
  self.queue_adapter = :inline
  def perform(*); end
end

class UniqueUntilExecutedJob < ActiveJob::Base
  include Pgbus::Uniqueness

  self.queue_adapter = :inline
  ensures_uniqueness strategy: :until_executed, lock_ttl: 3600, on_conflict: :discard
  def perform(*); end
end

class UniqueWhileExecutingJob < ActiveJob::Base
  include Pgbus::Uniqueness

  self.queue_adapter = :inline
  ensures_uniqueness strategy: :while_executing, lock_ttl: 3600, on_conflict: :discard
  def perform(*); end
end

# rubocop:enable Style/OneClassPerFile

# ─── Helpers ───

def purge_queue!
  Pgbus.client.purge_queue("default")
  Pgbus::JobLock.delete_all
rescue StandardError
  nil
end

def build_message(payload_hash)
  json = JSON.generate(payload_hash)
  Struct.new(:msg_id, :message, :read_ct, :headers, :enqueued_at)
        .new(1, json, 1, nil, Time.now.utc.iso8601)
end

client = Pgbus.client
adapter = Pgbus::ActiveJob::Adapter.new
executor = Pgbus::ActiveJob::Executor.new(client: client, config: Pgbus.configuration)

puts "=" * 70
puts "Pgbus Integration Benchmarks (real PostgreSQL)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}"
puts "=" * 70

# ═══════════════════════════════════════════════════════════════════════
# 1. Enqueue throughput
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Enqueue throughput (real DB) ---"
purge_queue!

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("plain enqueue") do
    client.send_message("default", { "job_class" => "PlainBenchJob", "arguments" => [1] })
  end

  x.report("enqueue + until_executed lock") do |times|
    times.times do |i|
      key = "bench-ue-#{i}-#{SecureRandom.hex(4)}"
      payload = {
        "job_class" => "UniqueUntilExecutedJob", "arguments" => [i],
        Pgbus::Uniqueness::METADATA_KEY => key,
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
        Pgbus::Uniqueness::TTL_KEY => 3600
      }
      Pgbus::JobLock.acquire!(key, job_class: "UniqueUntilExecutedJob", job_id: "b#{i}", ttl: 3600)
      client.send_message("default", payload)
    end
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 2. Full adapter enqueue (serialize + uniqueness + send)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Adapter#enqueue (full path, real DB) ---"
purge_queue!

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("adapter: plain job") do
    adapter.enqueue(PlainBenchJob.new(rand(1_000_000)))
  end

  # until_executed: each call needs a unique key to avoid conflict
  x.report("adapter: until_executed") do
    adapter.enqueue(UniqueUntilExecutedJob.new(SecureRandom.hex(8)))
  end

  x.report("adapter: while_executing") do
    adapter.enqueue(UniqueWhileExecutingJob.new(SecureRandom.hex(8)))
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 3. Execute throughput (read → deserialize → perform → archive)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Executor#execute (full path, real DB) ---"

# Pre-enqueue messages to read back
plain_payload = PlainBenchJob.new(42).serialize
plain_msg = build_message(plain_payload)

ue_payload = UniqueUntilExecutedJob.new(42).serialize
ue_payload[Pgbus::Uniqueness::METADATA_KEY] = "bench-exec-ue"
ue_payload[Pgbus::Uniqueness::STRATEGY_KEY] = "until_executed"
ue_payload[Pgbus::Uniqueness::TTL_KEY] = 3600
ue_msg = build_message(ue_payload)

we_payload = UniqueWhileExecutingJob.new(42).serialize
we_payload[Pgbus::Uniqueness::METADATA_KEY] = "bench-exec-we"
we_payload[Pgbus::Uniqueness::STRATEGY_KEY] = "while_executing"
we_payload[Pgbus::Uniqueness::TTL_KEY] = 3600
we_msg = build_message(we_payload)

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("execute: plain job") do
    executor.execute(plain_msg, "default")
  end

  x.report("execute: until_executed") do
    # Ensure lock exists for claim_for_execution
    Pgbus::JobLock.acquire!("bench-exec-ue", job_class: "UniqueUntilExecutedJob",
                                             job_id: "b1", state: "queued", ttl: 3600)
    executor.execute(ue_msg, "default")
  end

  x.report("execute: while_executing") do
    # Release any prior lock so acquire succeeds
    Pgbus::JobLock.release!("bench-exec-we")
    executor.execute(we_msg, "default")
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 4. Batch enqueue throughput
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Batch send_message (real DB) ---"
purge_queue!

small_batch = Array.new(10) { { "job_class" => "PlainBenchJob", "arguments" => [rand(1000)] } }
large_batch = Array.new(100) { { "job_class" => "PlainBenchJob", "arguments" => [rand(1000)] } }

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("send_batch(10)") { client.send_batch("default", small_batch) }
  x.report("send_batch(100)") { client.send_batch("default", large_batch) }

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 5. Lock operations (raw)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- JobLock operations (real DB) ---"
Pgbus::JobLock.delete_all

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("acquire + release") do |times|
    times.times do |i|
      key = "bench-lock-#{i}-#{SecureRandom.hex(4)}"
      Pgbus::JobLock.acquire!(key, job_class: "BenchJob", job_id: "j#{i}", ttl: 3600)
      Pgbus::JobLock.release!(key)
    end
  end

  x.report("acquire (conflict)") do |times|
    Pgbus::JobLock.acquire!("conflict-key", job_class: "BenchJob", job_id: "j0", ttl: 86_400)
    times.times do
      Pgbus::JobLock.acquire!("conflict-key", job_class: "BenchJob", job_id: "j1", ttl: 3600)
    end
    Pgbus::JobLock.release!("conflict-key")
  end

  x.compare!
end
Pgbus::JobLock.delete_all

# ═══════════════════════════════════════════════════════════════════════
# 6. Memory profile — full enqueue+execute cycle
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Memory: 500 enqueue+execute cycles (plain) ---"
purge_queue!
report = MemoryProfiler.report do
  500.times do
    client.send_message("default", plain_payload)
    executor.execute(plain_msg, "default")
  end
end
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-cycle:       ~#{report.total_allocated_memsize / 500} bytes (~#{report.total_allocated / 500} objects)"

puts "\n--- Memory: 500 enqueue+execute cycles (until_executed) ---"
purge_queue!
report = MemoryProfiler.report do
  500.times do |i|
    key = "mem-ue-#{i}"
    payload = ue_payload.merge(Pgbus::Uniqueness::METADATA_KEY => key)
    Pgbus::JobLock.acquire!(key, job_class: "UniqueUntilExecutedJob", job_id: "m#{i}",
                                 state: "queued", ttl: 3600)
    client.send_message("default", payload)
    msg = build_message(payload)
    executor.execute(msg, "default")
  end
end
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-cycle:       ~#{report.total_allocated_memsize / 500} bytes (~#{report.total_allocated / 500} objects)"

if report.total_retained.positive?
  puts "\nWARNING: retained objects detected — potential memory leak"
  report.retained_objects_by_location.first(5).each do |stat|
    puts "  #{stat[:data]}: #{stat[:count]} objects"
  end
end

purge_queue!
puts "\nDone."
