# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::CircuitBreaker do
  subject(:breaker) { described_class.new(config: config) }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.circuit_breaker_enabled = true
    end
  end

  let(:queue_state_class) do
    stub_const("Pgbus::QueueState", Class.new)
  end

  # Stub the tuning constants down to small values so we don't have to record
  # 5 failures and wait 30s in tests. THRESHOLD/BASE_BACKOFF/MAX_BACKOFF live
  # on Pgbus::CircuitBreaker (no longer config) — see PR that culled silent
  # configuration knobs.
  before do
    stub_const("Pgbus::CircuitBreaker::THRESHOLD", 3)
    stub_const("Pgbus::CircuitBreaker::BASE_BACKOFF", 10)
    stub_const("Pgbus::CircuitBreaker::MAX_BACKOFF", 60)
    queue_state_class
    allow(Pgbus::QueueState).to receive(:pause!)
    allow(Pgbus::QueueState).to receive(:resume!)
    allow(Pgbus::QueueState).to receive_messages(find_by: nil, paused?: false,
                                                 find_or_initialize_by: double("QueueState", update!: true, circuit_breaker_trip_count: 0))
  end

  describe "#record_success" do
    it "resets the failure counter" do
      breaker.record_failure("test_queue")
      breaker.record_success("test_queue")
      # After reset, we should be able to fail THRESHOLD-1 times without tripping
      (Pgbus::CircuitBreaker::THRESHOLD - 1).times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).not_to have_received(:find_or_initialize_by)
    end
  end

  describe "#record_failure" do
    it "trips after reaching threshold" do
      Pgbus::CircuitBreaker::THRESHOLD.times { breaker.record_failure("test_queue") }
      expect(Pgbus::QueueState).to have_received(:find_or_initialize_by).with(queue_name: "test_queue")
    end

    it "does not trip below threshold" do
      (Pgbus::CircuitBreaker::THRESHOLD - 1).times { breaker.record_failure("test_queue") }
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
