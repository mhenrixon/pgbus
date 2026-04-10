# frozen_string_literal: true

require_relative "bench_helper"

# Execution Pool Benchmark
# Compares ThreadPool vs AsyncPool throughput, latency, and memory.
#
# Usage:
#   ruby benchmarks/execution_pool_bench.rb

CAPACITIES = [10, 50].freeze

def build_thread_pool(capacity)
  Pgbus::ExecutionPools::ThreadPool.new(capacity: capacity)
end

def build_async_pool(capacity)
  Pgbus::ExecutionPools::AsyncPool.new(capacity: capacity)
end

# --- 1. No-op throughput (scheduling overhead) ---

puts "=" * 70
puts "1. No-op throughput — scheduling overhead (IPS)"
puts "=" * 70

CAPACITIES.each do |cap|
  Benchmark.ips do |x|
    x.config(warmup: 2, time: 5)

    x.report("ThreadPool(#{cap}) no-op") do
      pool = build_thread_pool(cap)
      done = Concurrent::CountDownLatch.new(cap)
      cap.times { pool.post { done.count_down } }
      done.wait(10)
      pool.shutdown
      pool.wait_for_termination(5)
    end

    x.report("AsyncPool(#{cap}) no-op") do
      pool = build_async_pool(cap)
      done = Concurrent::CountDownLatch.new(cap)
      cap.times { pool.post { done.count_down } }
      done.wait(10)
      pool.shutdown
      pool.wait_for_termination(5)
    end

    x.compare!
  end
  puts
end

# --- 2. I/O-bound throughput (fiber advantage zone) ---

puts "=" * 70
puts "2. I/O-bound throughput — sleep(0.01) simulating DB I/O"
puts "=" * 70

CAPACITIES.each do |cap|
  Benchmark.ips do |x|
    x.config(warmup: 1, time: 5)

    x.report("ThreadPool(#{cap}) I/O") do
      pool = build_thread_pool(cap)
      done = Concurrent::CountDownLatch.new(cap)
      cap.times do
        pool.post do
          sleep(0.01)
          done.count_down
        end
      end
      done.wait(10)
      pool.shutdown
      pool.wait_for_termination(5)
    end

    x.report("AsyncPool(#{cap}) I/O") do
      pool = build_async_pool(cap)
      done = Concurrent::CountDownLatch.new(cap)
      cap.times do
        pool.post do
          sleep(0.01)
          done.count_down
        end
      end
      done.wait(10)
      pool.shutdown
      pool.wait_for_termination(5)
    end

    x.compare!
  end
  puts
end

# --- 3. Memory overhead ---

puts "=" * 70
puts "3. Memory overhead — 1000 tasks"
puts "=" * 70

[10, 50].each do |cap|
  puts "\n--- Capacity: #{cap} ---"

  %i[threads async].each do |mode|
    report = MemoryProfiler.report do
      100.times do
        pool = if mode == :threads
                 build_thread_pool(cap)
               else
                 build_async_pool(cap)
               end
        done = Concurrent::CountDownLatch.new(cap)
        cap.times { pool.post { done.count_down } }
        done.wait(10)
        pool.shutdown
        pool.wait_for_termination(5)
      end
    end

    puts "\n#{mode.upcase} pool (#{cap} capacity × 100 iterations):"
    puts "  Total allocated: #{report.total_allocated_memsize} bytes"
    puts "  Total retained:  #{report.total_retained_memsize} bytes"
    puts "  Allocated objects: #{report.total_allocated}"
    puts "  Retained objects:  #{report.total_retained}"
  end
end

# --- 4. Latency percentiles ---

puts "\n#{"=" * 70}"
puts "4. Latency percentiles — single task completion time"
puts "=" * 70

SAMPLES = 500

%i[threads async].each do |mode|
  pool = mode == :threads ? build_thread_pool(5) : build_async_pool(5)
  latencies = []

  SAMPLES.times do
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    done = Concurrent::Event.new
    pool.post { done.set }
    done.wait(5)
    elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1_000_000 # microseconds
    latencies << elapsed
  end

  pool.shutdown
  pool.wait_for_termination(5)

  latencies.sort!
  p50 = latencies[(SAMPLES * 0.50).to_i]
  p95 = latencies[(SAMPLES * 0.95).to_i]
  p99 = latencies[(SAMPLES * 0.99).to_i]

  puts "\n#{mode.upcase} pool latency (#{SAMPLES} samples):"
  puts "  p50: #{p50.round(1)} µs"
  puts "  p95: #{p95.round(1)} µs"
  puts "  p99: #{p99.round(1)} µs"
  puts "  min: #{latencies.first.round(1)} µs"
  puts "  max: #{latencies.last.round(1)} µs"
end

puts "\nDone."
