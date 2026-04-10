# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ExecutionPools do
  describe ".build" do
    it "returns a ThreadPool for :threads mode" do
      pool = described_class.build(mode: :threads, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::ThreadPool)
      pool.kill
    end

    it "returns an AsyncPool for :async mode" do
      pool = described_class.build(mode: :async, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::AsyncPool)
      pool.shutdown
      pool.wait_for_termination(2)
    end

    it "returns an AsyncPool for :fiber mode (alias)" do
      pool = described_class.build(mode: :fiber, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::AsyncPool)
      pool.shutdown
      pool.wait_for_termination(2)
    end

    it "raises ArgumentError for unknown mode" do
      expect { described_class.build(mode: :unknown, capacity: 3) }
        .to raise_error(ArgumentError, /unknown execution_mode/i)
    end
  end

  describe ".normalize_mode" do
    it "normalizes :fiber to :async" do
      expect(described_class.normalize_mode(:fiber)).to eq(:async)
    end

    it "passes :threads through" do
      expect(described_class.normalize_mode(:threads)).to eq(:threads)
    end

    it "passes :async through" do
      expect(described_class.normalize_mode(:async)).to eq(:async)
    end

    it "accepts string modes" do
      expect(described_class.normalize_mode("threads")).to eq(:threads)
      expect(described_class.normalize_mode("async")).to eq(:async)
    end

    it "raises for unknown modes" do
      expect { described_class.normalize_mode(:bogus) }
        .to raise_error(ArgumentError)
    end
  end
end
