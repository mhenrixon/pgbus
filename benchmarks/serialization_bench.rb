# frozen_string_literal: true

require_relative "bench_helper"

puts "=" * 60
puts "Pgbus Serialization Benchmarks"
puts "=" * 60

small_json = JSON.generate(SMALL_PAYLOAD)
medium_json = JSON.generate(MEDIUM_PAYLOAD)
large_json = JSON.generate(LARGE_PAYLOAD)

puts "\n--- JSON.generate throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)
  x.report("small payload (#{small_json.bytesize}B)") { JSON.generate(SMALL_PAYLOAD) }
  x.report("medium payload (#{medium_json.bytesize}B)") { JSON.generate(MEDIUM_PAYLOAD) }
  x.report("large payload (#{large_json.bytesize}B)") { JSON.generate(LARGE_PAYLOAD) }
  x.compare!
end

puts "\n--- JSON.parse throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)
  x.report("small payload") { JSON.parse(small_json) }
  x.report("medium payload") { JSON.parse(medium_json) }
  x.report("large payload") { JSON.parse(large_json) }
  x.compare!
end

puts "\n--- Memory: serialize small payload ---"
report = MemoryProfiler.report { 1000.times { JSON.generate(SMALL_PAYLOAD) } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"

puts "\n--- Memory: serialize large payload ---"
report = MemoryProfiler.report { 1000.times { JSON.generate(LARGE_PAYLOAD) } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
