# frozen_string_literal: true

require "benchmark/ips"
require "memory_profiler"

# Shared harness for consistent benchmark reporting across all benchmarks.
# Provides throughput (benchmark-ips) + allocation tracking (memory_profiler)
# in a uniform format that the CI report-capture task can parse.
#
# Pure module — no side effects on require, so it's safe to load from specs.
# bench_helper.rb (which mutates Pgbus.configuration) is for benchmark scripts
# only and should never be required from the spec suite.
module BenchSupport
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
