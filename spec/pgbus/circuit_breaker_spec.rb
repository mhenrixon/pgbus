# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::CircuitBreaker do
  subject(:breaker) { described_class.new(config: config) }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.circuit_breaker_enabled = true
      c.circuit_breaker_threshold = 3
      c.circuit_breaker_base_backoff = 10
      c.circuit_breaker_max_backoff = 60
    end
  end

  let(:queue_state_class) do
    stub_const("Pgbus::QueueState", Class.new)
  end

  before do
    queue_state_class
    allow(Pgbus::QueueState).to receive(:find_by).and_return(nil)
    allow(Pgbus::QueueState).to receive(:paused?).and_return(false)
    allow(Pgbus::QueueState).to receive(:pause!)
    allow(Pgbus::QueueState).to receive(:resume!)
    allow(Pgbus::QueueState).to receive(:find_or_initialize_by).and_return(
      double("QueueState", update!: true, circuit_breaker_trip_count: 0)
    )
  end

  describe "#record_success" do
    it "resets the failure counter" do
      breaker.record_failure("test_queue")
      breaker.record_success("test_queue")
      # After reset, we should be able to fail threshold-1 times without tripping
      (config.circuit_breaker_threshold - 1).times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).not_to have_received(:find_or_initialize_by)
    end
  end

  describe "#record_failure" do
    it "trips after reaching threshold" do
      config.circuit_breaker_threshold.times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).to have_received(:find_or_initialize_by).with(queue_name: "test_queue")
    end

    it "does not trip below threshold" do
      (config.circuit_breaker_threshold - 1).times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).not_to have_received(:find_or_initialize_by)
    end

    it "does nothing when circuit breaker is disabled" do
      config.circuit_breaker_enabled = false
      10.times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).not_to have_received(:find_or_initialize_by)
    end
  end

  describe "#paused?" do
    it "returns false when queue is not paused" do
      expect(breaker.paused?("test_queue")).to be false
    end

    it "returns true when queue is paused" do
      state = double("QueueState", paused?: true, circuit_breaker_resume_at: nil)
      allow(Pgbus::QueueState).to receive(:find_by).with(queue_name: "test_queue").and_return(state)
      expect(breaker.paused?("test_queue")).to be true
    end

    it "auto-resumes when backoff has expired" do
      state = double("QueueState",
                     paused?: true,
                     circuit_breaker_resume_at: Time.now - 1,
                     update!: true)
      allow(Pgbus::QueueState).to receive(:find_by).with(queue_name: "test_queue").and_return(state)
      expect(breaker.paused?("test_queue")).to be false
    end

    it "caches the result for the TTL period" do
      allow(Pgbus::QueueState).to receive(:find_by).and_return(nil)
      breaker.paused?("test_queue")
      breaker.paused?("test_queue")
      expect(Pgbus::QueueState).to have_received(:find_by).once
    end
  end

  describe "#pause!" do
    it "delegates to QueueState.pause!" do
      breaker.pause!("test_queue", reason: "manual")
      expect(Pgbus::QueueState).to have_received(:pause!).with("test_queue", reason: "manual")
    end
  end

  describe "#resume!" do
    it "delegates to QueueState.resume! and clears failure counts" do
      breaker.record_failure("test_queue")
      breaker.resume!("test_queue")
      expect(Pgbus::QueueState).to have_received(:resume!).with("test_queue")
    end
  end

  describe "backoff calculation" do
    it "uses exponential backoff capped at max" do
      # First trip: base * 2^0 = 10
      # Second trip: base * 2^1 = 20
      # Third trip: base * 2^2 = 40
      # Fourth trip: base * 2^3 = 80 -> capped to 60
      backoff = breaker.send(:calculate_backoff, 1)
      expect(backoff).to eq(10)

      backoff = breaker.send(:calculate_backoff, 2)
      expect(backoff).to eq(20)

      backoff = breaker.send(:calculate_backoff, 4)
      expect(backoff).to eq(60) # capped
    end
  end
end
