# frozen_string_literal: true

require_relative "bench_helper"

puts "=" * 60
puts "Pgbus Client Benchmarks (mocked PGMQ — measures gem overhead)"
puts "=" * 60

client = BenchStubs.build_mock_client

puts "\n--- send_message throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("small payload") do
    client.send_message("default", SMALL_PAYLOAD)
  end

  x.report("large payload") do
    client.send_message("default", LARGE_PAYLOAD)
  end

  x.compare!
end

puts "\n--- send_batch throughput ---"
batch_ten = Array.new(10) { SMALL_PAYLOAD }
batch_hundred = Array.new(100) { SMALL_PAYLOAD }

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)
  x.report("batch of 10") { client.send_batch("default", batch_ten) }
  x.report("batch of 100") { client.send_batch("default", batch_hundred) }
  x.compare!
end

puts "\n--- read_batch throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)
  x.report("read_batch(5)") { client.read_batch("default", qty: 5) }
  x.report("read_batch(25)") { client.read_batch("default", qty: 25) }
  x.compare!
end

puts "\n--- Memory: send_message (1000 calls) ---"
report = MemoryProfiler.report { 1000.times { client.send_message("default", SMALL_PAYLOAD) } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-call allocated: ~#{report.total_allocated_memsize / 1000} bytes (~#{report.total_allocated / 1000} objects)"

puts "\n--- Memory: send_batch of 100 (100 calls) ---"
report = MemoryProfiler.report { 100.times { client.send_batch("default", batch_hundred) } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-call allocated: ~#{report.total_allocated_memsize / 100} bytes (~#{report.total_allocated / 100} objects)"
