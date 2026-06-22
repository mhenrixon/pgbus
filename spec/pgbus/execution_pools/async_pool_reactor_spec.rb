# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ExecutionPools::AsyncPool do
  subject(:pool) { described_class.new(capacity: capacity, on_state_change: state_change_cb) }

  let(:capacity) { 5 }
  let(:state_changes) { Concurrent::AtomicFixnum.new(0) }
  let(:state_change_cb) { -> { state_changes.increment } }

  after do
    pool.shutdown
    pool.wait_for_termination(5)
  end

  describe "reactor does not use blocking Kernel#sleep" do
    let(:source) do
      File.read(
        File.expand_path("../../../lib/pgbus/execution_pools/async_pool.rb", __dir__)
      )
    end

    it "wait_for_executions uses fiber-aware waiting" do
      wait_method = source[/def wait_for_executions.*?(?=\n      def |\n    end\b)/m]
      expect(wait_method).not_to be_nil
      bare_sleeps = wait_method.scan(/(?<!\.)\bsleep\b/)
      expect(bare_sleeps).to be_empty,
                             "wait_for_executions must not use blocking Kernel#sleep (issue #174)."
    end

    it "wait_for_inflight uses fiber-aware waiting" do
      inflight_method = source[/def wait_for_inflight.*?(?=\n      def |\n    end\b)/m]
      expect(inflight_method).not_to be_nil
      bare_sleeps = inflight_method.scan(/(?<!\.)\bsleep\b/)
      expect(bare_sleeps).to be_empty,
                             "wait_for_inflight must not use blocking Kernel#sleep (issue #174)."
    end
  end

  describe "notification-based wake on post" do
    it "executes posted work within 5ms even after extended idle" do
      warmup = Concurrent::IVar.new
      pool.post { warmup.set(:ok) }
      warmup.value(5)

      sleep 0.15

      latencies = []
      10.times do
        sleep 0.03

        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        ivar = Concurrent::IVar.new
        pool.post { ivar.set(Process.clock_gettime(Process::CLOCK_MONOTONIC)) }
        t1 = ivar.value(5)
        expect(t1).not_to be_nil, "Posted work was never executed (reactor stalled)"
        latencies << (t1 - t0)
      end

      sorted = latencies.sort
      p90 = sorted[(latencies.size * 0.9).floor]
      expect(p90).to be < 0.005,
                     "p90 post-to-execute latency was #{(p90 * 1000).round(2)}ms (expected <5ms)"
    end
  end

  describe "sustained burst-idle throughput" do
    let(:rounds) { 20 }
    let(:results) { Concurrent::Array.new }
    let(:stop_polling) { Concurrent::AtomicBoolean.new(false) }
    let(:poller) { start_contention_poller }

    after do
      stop_polling.make_true
      poller.join(1)
    end

    it "processes all tasks across burst-idle cycles without stalling" do
      poller # start

      rounds.times do |round|
        sleep 0.005
        latch = Concurrent::CountDownLatch.new(capacity)

        capacity.times do
          pool.post do
            results << round
            latch.count_down
          end
        end

        expect(latch.wait(2)).to be(true),
                                 "Round #{round}: stall detected"
      end

      expect(results.size).to eq(rounds * capacity)
    end

    private

    def start_contention_poller
      Thread.new do
        until stop_polling.true?
          begin
            pool.available_capacity
          rescue RuntimeError
            nil
          end
          Thread.pass
        end
      end
    end
  end
end
