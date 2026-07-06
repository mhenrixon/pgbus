# frozen_string_literal: true

require "spec_helper"

# Unit specs for the self-tuning streams-pool autoscaler (issue #323). Fully
# DB-free: the DB round-trip lives behind an injected `probe` that returns
# {maxc:, used:, peers:}, and the pool busy_ratio comes from a fake client's
# streams_pool_stats. The decision policy is driven by calling #tick directly
# with an injected clock, so thresholds/hysteresis/cooldown are deterministic.
RSpec.describe Pgbus::Web::Streamer::PoolAutoscaler do
  subject(:autoscaler) do
    described_class.new(client: client, config: config, probe: probe, logger: logger, clock: clock)
  end

  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil, debug: nil) }

  # Injected monotonic clock (seconds). Tests advance it explicitly.
  let(:now) { [0.0] }
  let(:clock) { -> { now[0] } }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.streams_pool_size = 3              # baseline / shrink floor
      c.streams_pool_max = nil             # no hard cap by default
      c.streams_pool_autoscale_interval = 1.0
    end
  end

  # Fake client: scripted busy_ratio via streams_pool_stats, records resize calls.
  let(:pool_size) { [3] }
  let(:pool_available) { [3] }
  let(:resizes) { [] }
  let(:client) do
    stats = -> { { size: pool_size[0], available: pool_available[0] } }
    instance_double(Pgbus::Client).tap do |c|
      allow(c).to receive(:streams_pool_stats) { stats.call }
      allow(c).to receive(:resize_streams_pool) do |target|
        resizes << target
        # Simulate the swap: new pool at the target size, freshly lazy (available == size).
        pool_size[0] = target
        pool_available[0] = target
        swap_result
      end
    end
  end
  # By default, resize returns a real swap (a SwapStats-like truthy that the
  # autoscaler recognizes via ResizablePool::SwapStats). Tests override for no-op.
  let(:swap_result) do
    Pgbus::Client::ResizablePool::SwapStats.new(
      swap_count: 1, last_drain_seconds: 0.01, last_conns_closed: 0,
      last_from_size: 3, last_to_size: 6, last_drained: true
    )
  end

  # Scripted probe: each tick pops the next {maxc,used,peers}, or repeats the last.
  let(:probe_script) { [] }
  let(:probe) do
    script = probe_script
    instance_double(described_class::HeadroomProbe, close: nil).tap do |p|
      allow(p).to receive(:read) { script.length > 1 ? script.shift : script.first }
    end
  end

  def busy!(ratio)
    size = pool_size[0]
    pool_available[0] = (size - (ratio * size).round).clamp(0, size)
  end

  def advance(seconds)
    now[0] += seconds
  end

  def healthy_probe(free: 60, peers: 5, maxc: 100)
    { maxc: maxc, used: maxc - free, peers: peers }
  end

  describe "#tick — GROW" do
    it "grows after 3 sustained high-busy samples, not before" do
      probe_script.replace([healthy_probe(free: 60, peers: 5)])
      busy!(1.0)

      expect(autoscaler.tick).to eq(:hold) # sample 1
      expect(autoscaler.tick).to eq(:hold) # sample 2
      expect(autoscaler.tick).to eq(:grow) # sample 3 -> grow
      expect(resizes.last).to be > 3       # grew above baseline
    end

    it "does not grow on a single busy spike" do
      probe_script.replace([healthy_probe])
      busy!(1.0)
      autoscaler.tick
      busy!(0.1) # spike passed
      autoscaler.tick

      expect(resizes).to be_empty
    end

    it "computes the grow target as size + a bounded fair share of headroom" do
      # free=40, peers=5, SAFETY 1.5, FAIR_FRACTION 0.25:
      # fair_share = 40/(5*1.5)=5.33 -> floor(0.25*5.33)=1 -> clamped by STEP_MAX 4, floor(free/2)=20 -> +1
      probe_script.replace([healthy_probe(free: 40, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes.last).to eq(4) # 3 + 1
    end

    it "grows by STEP_MAX at most, even when peers are undercounted (herd guard)" do
      # free=100, peers=1 (cold-boot undercount): floor(0.25*100/1.5)=16 clamped to STEP_MAX 4
      probe_script.replace([healthy_probe(free: 100, peers: 1)])
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes.last).to eq(7) # 3 + 4
    end

    it "does NOT grow when live headroom is below the grow reserve" do
      # free=10 < GROW_RESERVE (max(20, 0.20*100)=20)
      probe_script.replace([healthy_probe(free: 10, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes).to be_empty
    end

    it "respects an optional streams_pool_max hard cap" do
      config.streams_pool_max = 5
      probe_script.replace([healthy_probe(free: 100, peers: 1)]) # would target 7
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes.last).to eq(5) # capped
    end
  end

  describe "#tick — SHRINK" do
    before { pool_size[0] = 8 } # start grown

    it "shrinks toward baseline after 20 sustained low-busy samples" do
      probe_script.replace([healthy_probe])
      busy!(0.1)
      19.times { expect(autoscaler.tick).to eq(:hold) }
      expect(autoscaler.tick).to eq(:shrink)
      expect(resizes.last).to be < 8
      expect(resizes.last).to be >= config.streams_pool_size
    end

    it "never shrinks below the baseline" do
      pool_size[0] = 4 # just above baseline 3
      probe_script.replace([healthy_probe])
      busy!(0.1)
      20.times { autoscaler.tick }

      expect(resizes.last).to eq(3) # floor
    end
  end

  describe "#tick — EMERGENCY SHRINK (DB running out of connections)" do
    before { pool_size[0] = 8 }

    it "shrinks straight to baseline immediately when free is critically low, overriding busy_ratio" do
      # free=3 < EMERGENCY_MARGIN (max(5, 0.05*100)=5), and pool is SATURATED.
      probe_script.replace([healthy_probe(free: 3, peers: 5)])
      busy!(1.0) # saturated — normally would grow

      expect(autoscaler.tick).to eq(:emergency_shrink)
      expect(resizes.last).to eq(config.streams_pool_size) # straight to baseline
    end

    it "emergency-shrinks even during a post-swap cooldown (keys off DB free, not busy_ratio)" do
      # Trigger a grow to start a cooldown.
      pool_size[0] = 3
      probe_script.replace([healthy_probe(free: 60, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick } # grow -> cooldown active

      # Now the DB goes critical DURING cooldown.
      pool_size[0] = 7
      probe_script.replace([healthy_probe(free: 2, peers: 5)])
      advance(1.0) # still inside 15s cooldown

      expect(autoscaler.tick).to eq(:emergency_shrink)
      expect(resizes.last).to eq(config.streams_pool_size)
    end
  end

  describe "#tick — cooldown + BUG-0 immunity" do
    it "holds (no spurious shrink) during cooldown when the fresh pool reads busy_ratio ~= 0" do
      pool_size[0] = 3
      probe_script.replace([healthy_probe(free: 60, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick } # grow -> pool_size becomes 6, available == 6 (lazy, busy 0)
      resizes.clear

      # Inside cooldown, the lazy pool reads busy ~= 0. Must NOT shrink.
      advance(1.0)
      expect(autoscaler.tick).to eq(:cooldown)
      expect(resizes).to be_empty
    end

    it "resumes normal decisions after the cooldown window expires" do
      pool_size[0] = 3
      probe_script.replace([healthy_probe(free: 60, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick }
      resizes.clear

      advance(16.0) # past the 15s cooldown
      busy!(1.0)
      3.times { autoscaler.tick } # can grow again

      expect(resizes).not_to be_empty
    end
  end

  describe "#tick — fail-soft + guards" do
    it "holds when the probe returns nil (checkout timed out under saturation)" do
      probe_script.replace([nil])
      busy!(1.0)

      expect(autoscaler.tick).to eq(:hold)
      expect(resizes).to be_empty
    end

    it "does not grow when size is 1 (telemetry checkout would starve publishing)" do
      config.streams_pool_size = 1
      pool_size[0] = 1
      probe_script.replace([healthy_probe(free: 100, peers: 1)])
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes).to be_empty
    end

    it "holds and does not start a cooldown when resize returns a no-op (unchanged/shared)" do
      allow(client).to receive(:resize_streams_pool) do |target|
        resizes << target
        { swapped: false, reason: :unchanged }
      end
      pool_size[0] = 3
      probe_script.replace([healthy_probe(free: 60, peers: 5)])
      busy!(1.0)
      3.times { autoscaler.tick } # attempts grow; resize is a no-op

      # No cooldown started -> the counters were reset but the next high sample
      # can immediately re-attempt (no :cooldown lock).
      expect(autoscaler.tick).not_to eq(:cooldown)
    end
  end

  describe "convergence under concurrent growth (simulated fleet)" do
    it "shrinks the grow delta to zero as peers/used rise, never exceeding max_connections" do
      busy!(1.0)
      # Tick 1: cold boot, everyone sees free=100 peers=1 -> +4 (bounded)
      probe_script.replace([{ maxc: 100, used: 0, peers: 1 }])
      3.times { autoscaler.tick }
      first = resizes.last
      expect(first).to eq(7) # 3 + STEP_MAX 4

      # Tick 2: fleet connected -> free dropped, peers accurate -> tiny/no grow
      resizes.clear
      advance(16.0)
      probe_script.replace([{ maxc: 100, used: 88, peers: 10 }]) # free=12 < GROW_RESERVE 20
      busy!(1.0)
      3.times { autoscaler.tick }

      expect(resizes).to be_empty # headroom gate closed -> converged, no exhaustion
    end
  end

  describe "#start / #stop lifecycle" do
    it "is idempotent and safe to stop before start" do
      expect { autoscaler.stop }.not_to raise_error
      autoscaler.start
      autoscaler.start # idempotent
      expect { autoscaler.stop }.not_to raise_error
    end
  end
end
