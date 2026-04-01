# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::RateCounter do
  subject(:counter) { described_class.new(:processed, :failed) }

  describe "#increment" do
    it "increments the named counter" do
      counter.increment(:processed)
      counter.increment(:processed)
      expect(counter.count(:processed)).to eq(2)
    end

    it "accepts a delta" do
      counter.increment(:processed, 5)
      expect(counter.count(:processed)).to eq(5)
    end

    it "raises for unknown counter names" do
      expect { counter.increment(:unknown) }.to raise_error(KeyError)
    end
  end

  describe "#count" do
    it "returns 0 for fresh counters" do
      expect(counter.count(:processed)).to eq(0)
      expect(counter.count(:failed)).to eq(0)
    end
  end

  describe "#counts" do
    it "returns all counters as a hash" do
      counter.increment(:processed, 10)
      counter.increment(:failed, 2)
      expect(counter.counts).to eq(processed: 10, failed: 2)
    end
  end

  describe "#snapshot! and #rate" do
    it "computes rate as delta / elapsed" do
      counter.increment(:processed, 100)
      # Simulate time passing by manipulating the internal monotonic timestamp
      counter.instance_variable_set(:@last_snapshot_at,
                                    Process.clock_gettime(Process::CLOCK_MONOTONIC) - 10.0)
      counter.instance_variable_set(:@last_values, { processed: 0, failed: 0 })
      counter.snapshot!

      # 100 msgs over 10 seconds ≈ 10.0 msgs/s
      expect(counter.rate(:processed)).to be_within(1.0).of(10.0)
      expect(counter.rate(:failed)).to eq(0.0)
    end

    it "returns 0 before first snapshot" do
      expect(counter.rate(:processed)).to eq(0.0)
    end
  end

  describe "#rates" do
    it "returns all rates as a hash" do
      expect(counter.rates).to eq(processed: 0.0, failed: 0.0)
    end
  end

  describe "#to_h" do
    it "returns counts and rates" do
      counter.increment(:processed, 5)
      result = counter.to_h
      expect(result[:counts]).to eq(processed: 5, failed: 0)
      expect(result[:rates]).to eq(processed: 0.0, failed: 0.0)
    end
  end

  describe "#names" do
    it "returns the configured counter names" do
      expect(counter.names).to eq(%i[processed failed])
    end
  end
end
