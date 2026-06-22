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

  # Block until the pool has a free slot, mirroring the worker's idle<=0
  # backpressure so a tight post loop never overruns capacity.
  def wait_for_capacity(pool, timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    Thread.pass until pool.available_capacity.positive? ||
                      Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
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

      # Assert on the MEDIAN, not a tail percentile: scheduler jitter on a
      # loaded CI box inflates the tail, but the median exposes the steady-state
      # mechanism. Polling sleep(0.01) floors the median near ~5ms (you wait, on
      # average, half a poll cycle); notification wake pushes it sub-millisecond.
      # A median < 3ms is unreachable with the old blocking-sleep poll loop.
      sorted = latencies.sort
      median = sorted[latencies.size / 2]
      expect(median).to be < 0.003,
                        "median post-to-execute latency was #{(median * 1000).round(2)}ms " \
                        "(expected <3ms; blocking-sleep polling floors this near 5ms)"
    end
  end

  # The worker's main loop parks in WakeSignal#wait between fetches. The ONLY
  # thing that wakes it when the pool is saturated is the on_state_change
  # callback fired as fibers complete and free capacity (Worker wires it to
  # WakeSignal#notify!). PR #164 (NOTIFY-gated wakeup) raises that wait to a
  # 15s fallback ceiling, so a missed on_state_change strands work for up to
  # 15s under execution_mode: :async. These tests pin the wake contract so the
  # 15s ceiling stays a safety net, never the steady-state latency.
  describe "on_state_change wake contract (de-risks #164 async fallback)" do
    it "fires on_state_change for every fiber that frees capacity" do
      total = 50
      done = Concurrent::CountDownLatch.new(total)

      # Post within capacity: each task completes quickly and frees a slot
      # before the next post, mirroring the worker's idle<=0 backpressure.
      total.times do |i|
        wait_for_capacity(pool)
        pool.post do
          done.count_down
        end
        # Yield so the reactor schedules and the fiber completes between posts.
        Thread.pass if (i % capacity).zero?
      end

      expect(done.wait(5)).to be(true), "Not all fibers completed"

      # Each completion restores capacity and pokes on_state_change. With
      # serialized within-capacity posting, every task frees a slot from a
      # saturated-or-near-saturated state, so the callback must fire at least
      # once per task — never fewer (a dropped poke = a missed worker wake).
      expect(state_changes.value).to be >= total
    end

    it "wakes a saturated worker-style waiter the moment capacity frees" do
      # Saturate the pool with blocking work, then park a waiter exactly as the
      # worker loop does: WakeSignal#wait with a long (15s-style) timeout that
      # must be interrupted early by on_state_change, not by the timeout.
      wake_signal = Pgbus::Process::WakeSignal.new
      saturating_pool = described_class.new(capacity: 2, on_state_change: -> { wake_signal.notify! })
      release = Concurrent::Event.new
      saturate(saturating_pool, count: 2, release: release)

      woke_after = Concurrent::IVar.new
      waiter = park_waiter(wake_signal, woke_after, timeout: 15) # the #164 fallback ceiling
      sleep 0.05
      release.set # fibers finish → restore_capacity → on_state_change → notify!

      elapsed = woke_after.value(5)
      expect(elapsed).not_to be_nil, "Waiter never woke (stranded on the 15s ceiling)"
      expect(elapsed).to be < 1.0,
                         "Waiter woke after #{elapsed.round(2)}s — on_state_change " \
                         "did not interrupt the 15s fallback wait (issue #164 risk)"
    ensure
      waiter&.kill
      saturating_pool&.shutdown
      saturating_pool&.wait_for_termination(2)
    end

    def saturate(pool, count:, release:)
      count.times { pool.post { release.wait(5) } }
      sleep 0.05
      expect(pool.available_capacity).to eq(0)
    end

    def park_waiter(wake_signal, woke_after, timeout:)
      Thread.new do
        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        wake_signal.wait(timeout: timeout)
        woke_after.set(Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0)
      end
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
