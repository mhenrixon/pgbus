# frozen_string_literal: true

require "spec_helper"
require "concurrent"

RSpec.describe Pgbus::Client::ResizablePool do
  subject(:resizable) do
    described_class.new(seed_pgmq, shared: false, drain_timeout: 2.0, logger: logger, clock: clock)
  end

  let(:logger) { instance_double(Logger, warn: nil, error: nil) }
  # Injected clock so drain timeout is deterministic (no wall-clock waiting).
  let(:clock) { class_double(Process, clock_gettime: 0.0) }
  let(:seed_pgmq) { build_pgmq(size: 5, available: 5) }

  # A fake PGMQ::Client recording produce/with_connection/close and exposing a
  # controllable stats hash. `on_produce` lets a test park inside produce to hold
  # the in-flight counter high.
  def build_pgmq(size: 5, available: 5, on_produce: nil)
    stats = { size: size, available: available }
    Class.new do
      attr_reader :produced, :closed

      define_method(:initialize) do
        @produced = 0
        @closed = false
        @stats = stats
        @on_produce = on_produce
      end

      define_method(:produce) do |*_args, **_kwargs|
        @on_produce&.call
        @produced += 1
        1
      end

      define_method(:with_connection) do |&blk|
        blk&.call(:conn)
      end

      define_method(:stats) { @stats }
      define_method(:close) { @closed = true }
      define_method(:closed?) { @closed }
    end.new
  end

  describe "#current" do
    it "returns the seed pool, then the swapped pool after #swap" do
      expect(resizable.current.pgmq).to be(seed_pgmq)

      new_pgmq = build_pgmq(size: 12, available: 12)
      resizable.swap(new_pgmq, from_size: 5, to_size: 12)

      expect(resizable.current.pgmq).to be(new_pgmq)
    end
  end

  describe "in-flight tracking" do
    it "holds inflight at 1 during produce and 0 after" do
      observed = nil
      pool = nil
      pgmq = build_pgmq(on_produce: -> { observed = pool.current.inflight.value })
      pool = described_class.new(pgmq, shared: false, drain_timeout: 2.0, logger: logger, clock: clock)

      pool.produce("q", "payload")

      expect(observed).to eq(1)
      expect(pool.current.inflight.value).to eq(0)
    end

    it "brackets with_connection the same way" do
      observed = nil
      allow(seed_pgmq).to receive(:with_connection) do |&_blk|
        observed = resizable.current.inflight.value
      end

      resizable.with_connection { :x }

      expect(observed).to eq(1)
      expect(resizable.current.inflight.value).to eq(0)
    end
  end

  describe "#swap" do
    it "installs the new pool ref BEFORE draining/closing the old one" do
      new_pgmq = build_pgmq(size: 12, available: 12)
      ref_during_close = nil
      allow(seed_pgmq).to receive(:close) { ref_during_close = resizable.current.pgmq }

      resizable.swap(new_pgmq, from_size: 5, to_size: 12)

      expect(ref_during_close).to be(new_pgmq) # ref already swapped when old closes
      expect(seed_pgmq).to have_received(:close)
    end

    # THE regression test for the data-loss bug: drain must wait on the in-flight
    # counter, NOT connection_pool's available==size. A pgmq with_connection retry
    # checks a connection in between attempts, so available momentarily == size
    # while a produce is still mid-flight; closing then would lose the broadcast.
    it "does NOT close the old pool while a produce is in flight, even if stats read fully-available" do
      # Old pgmq reports available==size (looks drained) but a producer is parked
      # inside produce holding inflight at 1.
      gate = Queue.new
      parked = Queue.new
      old = build_pgmq(size: 5, available: 5, on_produce: lambda {
        parked << :in
        gate.pop # block inside produce until released
      })
      pool = described_class.new(old, shared: false, drain_timeout: 5.0, logger: logger)

      producer = Thread.new { pool.produce("q", "p") }
      parked.pop # ensure the producer is inside produce (inflight == 1)

      swapper = Thread.new { pool.swap(build_pgmq(size: 12, available: 12), from_size: 5, to_size: 12) }
      sleep 0.05
      # Even though old.stats says available==size, the swap must NOT have closed
      # the old pool yet — inflight is still 1.
      expect(old.closed?).to be(false)

      gate << :go # let the produce finish → inflight → 0
      swapper.join(2)
      producer.join(2)
      expect(old.closed?).to be(true) # now drained and closed
    end

    it "increments swap_count and records from/to sizes in telemetry" do
      resizable.swap(build_pgmq(size: 12, available: 12), from_size: 5, to_size: 12)

      stats = resizable.stats_snapshot
      expect(stats.swap_count).to eq(1)
      expect(stats.last_from_size).to eq(5)
      expect(stats.last_to_size).to eq(12)
    end

    it "reports last_conns_closed from the old pool's busy count at close time" do
      # size 12, available 9 → 3 connections open and about to be torn down.
      old = build_pgmq(size: 12, available: 9)
      pool = described_class.new(old, shared: false, drain_timeout: 2.0, logger: logger, clock: clock)

      pool.swap(build_pgmq(size: 3, available: 3), from_size: 12, to_size: 3)

      expect(pool.stats_snapshot.last_conns_closed).to eq(3)
    end

    it "serializes concurrent swaps and closes each retired pool exactly once" do
      a = build_pgmq(size: 6, available: 6)
      b = build_pgmq(size: 8, available: 8)

      t1 = Thread.new { resizable.swap(a, from_size: 5, to_size: 6) }
      t2 = Thread.new { resizable.swap(b, from_size: 6, to_size: 8) }
      [t1, t2].each { |t| t.join(2) }

      expect(resizable.stats_snapshot.swap_count).to eq(2)
      expect(seed_pgmq.closed?).to be(true)
      # a and b: whichever ended up current is NOT closed; the other IS.
      current = resizable.current.pgmq
      [a, b].each { |p| expect(p.closed?).to eq(!p.equal?(current)) }
    end
  end

  describe "#swap drain timeout" do
    it "warns, still closes, and marks last_drained false when inflight never reaches zero" do
      # Injected clock jumps past drain_timeout on the second reading.
      times = [0.0, 0.0, 100.0] # start, first poll, second poll (past 2.0s deadline)
      stepping_clock = class_double(Process)
      allow(stepping_clock).to receive(:clock_gettime) { times.shift || 100.0 }

      old = build_pgmq(size: 5, available: 5)
      old.instance_variable_get(:@stats) # touch
      pool = described_class.new(old, shared: false, drain_timeout: 2.0, logger: logger, clock: stepping_clock)
      # Force inflight to look stuck at 1 by incrementing the current pool's counter.
      pool.current.inflight.increment

      pool.swap(build_pgmq(size: 12, available: 12), from_size: 5, to_size: 12)

      expect(logger).to have_received(:warn)
      expect(old.closed?).to be(true) # closed anyway (leak-free: in-flight self-closes on checkin)
      expect(pool.stats_snapshot.last_drained).to be(false)
    end
  end

  describe "#close_current" do
    it "closes the current pool unless it aliases the job pool" do
      resizable.close_current(job_pool: build_pgmq)
      expect(seed_pgmq.closed?).to be(true)
    end

    it "does NOT close when the current pool IS the job pool (shared-AR alias guard)" do
      shared = described_class.new(seed_pgmq, shared: true, drain_timeout: 2.0, logger: logger, clock: clock)
      shared.close_current(job_pool: seed_pgmq)
      expect(seed_pgmq.closed?).to be(false)
    end
  end

  describe "#stats_snapshot" do
    it "returns a zero-value struct before any swap (never nil)" do
      snap = resizable.stats_snapshot
      expect(snap.swap_count).to eq(0)
      expect(snap.last_drain_seconds).to eq(0.0)
    end
  end

  describe "#stats" do
    it "delegates to the current pool's pgmq stats" do
      expect(resizable.stats).to eq(size: 5, available: 5)
    end
  end
end
