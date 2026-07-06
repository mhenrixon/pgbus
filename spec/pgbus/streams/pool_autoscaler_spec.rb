# frozen_string_literal: true

require "spec_helper"

# Unit specs for the self-tuning streams-pool autoscaler decision object (issue
# #323). It owns no thread and no connection — #evaluate takes a headroom hash
# ({maxc:, used:, peers:}) and the pool busy_ratio (from a fake client's
# streams_pool_stats) and decides grow/shrink/emergency/hold. Fully DB-free.
RSpec.describe Pgbus::Streams::PoolAutoscaler do
  subject(:autoscaler) { described_class.new(client: client, config: config, logger: logger) }

  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil, debug: nil) }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.streams_pool_size = 3   # baseline / shrink floor
      c.streams_pool_max = nil  # no hard cap by default
    end
  end

  let(:pool_size) { [3] }
  let(:pool_available) { [3] }
  let(:resizes) { [] }
  let(:client) do
    stats = -> { { size: pool_size[0], available: pool_available[0] } }
    instance_double(Pgbus::Client).tap do |c|
      allow(c).to receive(:streams_pool_stats) { stats.call }
      allow(c).to receive(:resize_streams_pool) do |target|
        resizes << target
        pool_size[0] = target
        pool_available[0] = target # fresh pool is lazy: available == size
        swap_result
      end
    end
  end
  let(:swap_result) do
    Pgbus::Client::ResizablePool::SwapStats.new(
      swap_count: 1, last_drain_seconds: 0.01, last_conns_closed: 0,
      last_from_size: 3, last_to_size: 6, last_drained: true
    )
  end

  def busy!(ratio)
    size = pool_size[0]
    pool_available[0] = (size - (ratio * size).round).clamp(0, size)
  end

  def headroom(free: 60, peers: 5, maxc: 100)
    { maxc: maxc, used: maxc - free, peers: peers }
  end

  describe "#evaluate — GROW" do
    it "grows on a saturated pool with a fair share of headroom" do
      busy!(1.0)
      expect(autoscaler.evaluate(headroom(free: 60, peers: 5))).to eq(:grow)
      expect(resizes.last).to be > 3
    end

    it "computes the grow target as size + a bounded fair share" do
      # free=40, peers=5, SAFETY 1.5, FAIR_FRACTION 0.25 → floor(0.25*40/(5*1.5))=1
      busy!(1.0)
      autoscaler.evaluate(headroom(free: 40, peers: 5))
      expect(resizes.last).to eq(4) # 3 + 1
    end

    it "caps a single grow at STEP_MAX even when peers are undercounted" do
      # free=100, peers=1 → floor(0.25*100/1.5)=16 clamped to STEP_MAX 4
      busy!(1.0)
      autoscaler.evaluate(headroom(free: 100, peers: 1))
      expect(resizes.last).to eq(7) # 3 + 4
    end

    it "does not grow when headroom is below the grow reserve" do
      busy!(1.0)
      # free=10 < GROW_RESERVE max(20, 0.20*100)=20
      expect(autoscaler.evaluate(headroom(free: 10, peers: 5))).to eq(:hold)
      expect(resizes).to be_empty
    end

    it "respects an optional streams_pool_max hard cap" do
      config.streams_pool_max = 5
      busy!(1.0)
      autoscaler.evaluate(headroom(free: 100, peers: 1)) # would target 7
      expect(resizes.last).to eq(5)
    end

    it "does not grow when the pool size is 1 (telemetry checkout would starve publishing)" do
      config.streams_pool_size = 1
      pool_size[0] = 1
      busy!(1.0)
      expect(autoscaler.evaluate(headroom(free: 100, peers: 1))).to eq(:hold)
      expect(resizes).to be_empty
    end
  end

  describe "#evaluate — SHRINK" do
    before { pool_size[0] = 8 }

    it "shrinks one step toward baseline when the pool is idle" do
      busy!(0.1)
      expect(autoscaler.evaluate(headroom)).to eq(:shrink)
      expect(resizes.last).to be < 8
      expect(resizes.last).to be >= config.streams_pool_size
    end

    it "never shrinks below the baseline" do
      pool_size[0] = 4
      busy!(0.1)
      autoscaler.evaluate(headroom)
      expect(resizes.last).to eq(3)
    end

    it "holds in the dead-band (neither saturated nor idle)" do
      busy!(0.5)
      expect(autoscaler.evaluate(headroom)).to eq(:hold)
      expect(resizes).to be_empty
    end
  end

  describe "#evaluate — EMERGENCY SHRINK" do
    before { pool_size[0] = 8 }

    it "shrinks straight to baseline when free is critically low, overriding the busy signal" do
      busy!(1.0) # saturated — would normally grow
      # free=3 < EMERGENCY_MARGIN max(5, 0.05*100)=5
      expect(autoscaler.evaluate(headroom(free: 3, peers: 5))).to eq(:emergency_shrink)
      expect(resizes.last).to eq(config.streams_pool_size)
    end

    it "is a no-op when already at baseline" do
      pool_size[0] = 3
      expect(autoscaler.evaluate(headroom(free: 2, peers: 5))).to eq(:hold)
      expect(resizes).to be_empty
    end

    it "warns and returns :hold when a critical-headroom shrink resize is a no-op" do
      # DB critically low AND above baseline, but the resize returns unswapped.
      allow(client).to receive(:resize_streams_pool) do |target|
        resizes << target
        { swapped: false, reason: :unchanged }
      end

      expect(autoscaler.evaluate(headroom(free: 2, peers: 5))).to eq(:hold)
      expect(logger).to have_received(:warn).at_least(:once)
    end
  end

  describe "#evaluate — fail-soft" do
    it "holds when headroom is nil (the maintenance query failed)" do
      busy!(1.0)
      expect(autoscaler.evaluate(nil)).to eq(:hold)
      expect(resizes).to be_empty
    end

    it "holds (no cooldown lock) when resize returns a no-op" do
      allow(client).to receive(:resize_streams_pool) do |target|
        resizes << target
        { swapped: false, reason: :unchanged }
      end
      busy!(1.0)
      expect(autoscaler.evaluate(headroom(free: 60, peers: 5))).to eq(:hold)
    end
  end

  describe "convergence across periodic checks (simulated fleet)" do
    it "grows once then stops as peers/used rise — never exceeds max_connections" do
      busy!(1.0)
      # Check 1: cold boot, free=100 peers=1 → +STEP_MAX
      autoscaler.evaluate(maxc: 100, used: 0, peers: 1)
      expect(resizes.last).to eq(7)

      # Check 2 (5 min later): fleet connected, headroom gone → no grow
      resizes.clear
      busy!(1.0)
      expect(autoscaler.evaluate(maxc: 100, used: 88, peers: 10)).to eq(:hold) # free=12 < reserve
      expect(resizes).to be_empty
    end
  end

  describe described_class::Maintenance do
    subject(:maintenance) do
      described_class.new(autoscaler: autoscaler_double, interval: 300, application_name_prefix: "pgbus_streams")
    end

    let(:autoscaler_double) { instance_double(Pgbus::Streams::PoolAutoscaler, evaluate: :hold) }

    it "runs the headroom query on the given connection and hands the reading to the autoscaler" do
      row = { "maxc" => "100", "used" => "40", "peers" => "5" }
      # PG isn't loaded in unit specs, so use plain doubles for the conn/result.
      result = double("PG::Result", first: row)
      conn = double("PG::Connection")
      allow(conn).to receive(:exec_params).and_return(result)

      maintenance.run(conn)

      expect(conn).to have_received(:exec_params).with(
        Pgbus::Streams::PoolAutoscaler::HEADROOM_SQL, ["pgbus_streams_%"]
      )
      expect(autoscaler_double).to have_received(:evaluate).with(maxc: 100, used: 40, peers: 5)
    end

    it "exposes the throttle interval" do
      expect(maintenance.interval).to eq(300)
    end
  end
end
