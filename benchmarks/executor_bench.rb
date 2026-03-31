# frozen_string_literal: true

require_relative "bench_helper"
require "active_job"

puts "=" * 60
puts "Pgbus Executor Benchmarks (mocked PGMQ — measures gem overhead)"
puts "=" * 60

# Minimal ActiveJob for benchmarking
class BenchJob < ActiveJob::Base
  self.queue_adapter = :inline

  def perform(*)
    # no-op
  end
end

client = BenchStubs.build_mock_client
executor = Pgbus::ActiveJob::Executor.new(client: client, config: Pgbus.configuration)

# Build a realistic message
job = BenchJob.new(42)
job_payload = job.serialize
message_json = JSON.generate(job_payload)

mock_message = Struct.new(:msg_id, :message, :read_ct, :headers)
msg = mock_message.new(1, message_json, 1, nil)

puts "\n--- Executor#execute throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("execute (small job)") do
    executor.execute(msg, "default")
  end

  x.compare!
end

puts "\n--- Memory: execute (1000 jobs) ---"
report = MemoryProfiler.report { 1000.times { executor.execute(msg, "default") } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-job allocated: ~#{report.total_allocated_memsize / 1000} bytes (~#{report.total_allocated / 1000} objects)"
puts "Per-job retained:  ~#{report.total_retained_memsize / 1000} bytes (~#{report.total_retained / 1000} objects)"

puts "\n--- Top allocations by gem ---"
report.pretty_print(to_file: "/dev/stdout", scale_bytes: true, detailed_report: false)
