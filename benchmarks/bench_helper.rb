# frozen_string_literal: true

require "benchmark/ips"
require "memory_profiler"
require "json"
require "securerandom"
require "concurrent"
require "pgbus"

# Suppress logging during benchmarks
Pgbus.configure do |c|
  c.logger = Logger.new(IO::NULL)
  c.queue_prefix = "pgbus_bench"
  c.default_queue = "default"
  c.stats_enabled = false # Don't record stats during benchmarks
end

# Stub PGMQ to isolate Pgbus overhead from database I/O.
# This measures the gem's own CPU and memory cost.
module BenchStubs
  def self.build_mock_pgmq
    MockPgmq.new
  end

  def self.build_mock_client
    pgmq = build_mock_pgmq
    Pgbus::Client.allocate.tap do |client|
      client.instance_variable_set(:@pgmq, pgmq)
      client.instance_variable_set(:@config, Pgbus.configuration)
      client.instance_variable_set(:@pgmq_mutex, Mutex.new)
      client.instance_variable_set(:@queues_created, Concurrent::Map.new.tap { |m| m["pgbus_bench_default"] = true })
      client.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
      client.instance_variable_set(:@schema_ensured, true)
      client.instance_variable_set(:@shared_connection, false)
    end
  end

  class MockPgmq
    def create(...) = nil
    def enable_notify_insert(...) = nil
    def produce(...) = 1
    def produce_batch(_, msgs, **) = (1..msgs.size).to_a
    def read(...) = nil
    def read_batch(*, **) = []
    def read_with_poll(...) = []
    def delete(...) = true # rubocop:disable Naming/PredicateMethod
    def archive(...) = true # rubocop:disable Naming/PredicateMethod
    def set_vt(...) = nil
    def metrics(...) = nil
    def metrics_all(...) = []
    def list_queues(...) = []
    def purge_queue(...) = nil
    def bind_topic(...) = nil
    def produce_topic(...) = nil
    def close(...) = nil
    def transaction(...) = yield(self)
  end
end

# Shared harness for consistent benchmark reporting across all benchmarks.
# Provides throughput (benchmark-ips) + allocation tracking (memory_profiler)
# in a uniform format that the CI report-capture task can parse.
module BenchSupport # rubocop:disable Style/OneClassPerFile
  module_function

  def ips(time: 2, warmup: 1, &)
    Benchmark.ips do |x|
      x.config(time: time, warmup: warmup)
      yield x
      x.compare!
    end
  end

  def allocations(label, &)
    report = MemoryProfiler.report(&)
    puts format(
      "  %-32s %8d objects  %10d bytes (retained: %d objects)",
      label, report.total_allocated, report.total_allocated_memsize, report.total_retained
    )
    report
  end

  def header(title)
    puts "\n\e[1;36m#{title}\e[0m"
    puts "─" * title.length
  end
end

# Sample payloads of varying sizes
SMALL_PAYLOAD = { "job_class" => "SmallJob", "job_id" => SecureRandom.uuid,
                  "queue_name" => "default", "arguments" => [1] }.freeze
MEDIUM_PAYLOAD = SMALL_PAYLOAD.merge("arguments" => (1..100).to_a).freeze
LARGE_PAYLOAD = SMALL_PAYLOAD.merge("arguments" => [{ "data" => "x" * 10_000 }]).freeze
