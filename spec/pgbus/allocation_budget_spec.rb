# frozen_string_literal: true

require "spec_helper"
require "memory_profiler"

RSpec.describe Pgbus::Client do
  include PgmqDoubles

  let(:mock_pgmq) { build_mock_pgmq }
  let(:all_queues_created) do
    Concurrent::Map.new.tap do |m|
      full = Pgbus.configuration.queue_name("default")
      m[full] = true
    end
  end
  let(:client) do
    described_class.allocate.tap do |c|
      c.instance_variable_set(:@pgmq, mock_pgmq)
      c.instance_variable_set(:@config, Pgbus.configuration)
      c.instance_variable_set(:@pgmq_mutex, Mutex.new)
      c.instance_variable_set(:@queues_created, all_queues_created)
      c.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
    end
  end

  let(:small_payload) do
    { "job_class" => "TestJob", "job_id" => "abc-123", "queue_name" => "default", "arguments" => [1] }
  end

  def count_allocations(&block)
    report = MemoryProfiler.report(&block)
    report.total_allocated
  end

  describe "#send_message" do
    it "allocates fewer than 50 objects per call" do
      5.times { client.send_message("default", small_payload) }

      allocs = count_allocations { client.send_message("default", small_payload) }

      expect(allocs).to be < 50
    end
  end

  describe "#send_batch" do
    let(:batch) { Array.new(10) { small_payload } }

    it "allocates fewer than 25 objects per item in a batch of 10" do
      5.times { client.send_batch("default", batch) }

      allocs = count_allocations { client.send_batch("default", batch) }
      per_item = allocs / 10.0

      expect(per_item).to be < 25
    end
  end

  describe "#read_batch" do
    it "allocates fewer than 30 objects per call" do
      5.times { client.read_batch("default", qty: 5) }

      allocs = count_allocations { client.read_batch("default", qty: 5) }

      expect(allocs).to be < 30
    end
  end

  describe "JSON serialization round-trip" do
    it "allocates fewer than 20 objects for generate + parse" do
      json = JSON.generate(small_payload)
      5.times { JSON.parse(json) }

      allocs = count_allocations do
        s = JSON.generate(small_payload)
        JSON.parse(s)
      end

      expect(allocs).to be < 20
    end
  end

  describe "no retained objects (leak detection)" do
    let(:plain_pgmq) do
      Class.new do
        def create(...) = nil
        def enable_notify_insert(...) = nil
        def produce(...) = 1
        def read(...) = nil
        def read_batch(*, **) = []
        def delete(...) = true
        def archive(...) = true
        def set_vt(...) = nil
        def close(...) = nil
        def transaction(...) = yield(self)
      end.new
    end

    let(:plain_client) do
      described_class.allocate.tap do |c|
        c.instance_variable_set(:@pgmq, plain_pgmq)
        c.instance_variable_set(:@config, Pgbus.configuration)
        c.instance_variable_set(:@pgmq_mutex, Mutex.new)
        c.instance_variable_set(:@queues_created, all_queues_created)
        c.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
      end
    end

    it "retains zero objects across 100 send_message cycles" do
      10.times { plain_client.send_message("default", small_payload) }

      report = MemoryProfiler.report do
        100.times { plain_client.send_message("default", small_payload) }
      end

      expect(report.total_retained).to eq(0)
    end
  end
end
