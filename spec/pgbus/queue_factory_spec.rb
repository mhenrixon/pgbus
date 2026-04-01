# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::QueueFactory do
  let(:config) { Pgbus::Configuration.new }

  describe ".for" do
    it "returns StandardStrategy when priority_levels is nil" do
      expect(described_class.for(config)).to be_a(described_class::StandardStrategy)
    end

    it "returns StandardStrategy when priority_levels is 1" do
      config.priority_levels = 1
      expect(described_class.for(config)).to be_a(described_class::StandardStrategy)
    end

    it "returns PriorityStrategy when priority_levels > 1" do
      config.priority_levels = 3
      expect(described_class.for(config)).to be_a(described_class::PriorityStrategy)
    end
  end

  describe Pgbus::QueueFactory::StandardStrategy do
    subject(:strategy) { described_class.new(config) }

    describe "#physical_queue_names" do
      it "returns a single prefixed queue name" do
        expect(strategy.physical_queue_names("default")).to eq(["pgbus_default"])
      end
    end

    describe "#target_queue" do
      it "returns the prefixed name regardless of priority" do
        expect(strategy.target_queue("default", nil)).to eq("pgbus_default")
        expect(strategy.target_queue("default", 0)).to eq("pgbus_default")
        expect(strategy.target_queue("default", 5)).to eq("pgbus_default")
      end
    end

    describe "#priority?" do
      it "returns false" do
        expect(strategy.priority?).to be false
      end
    end
  end

  describe Pgbus::QueueFactory::PriorityStrategy do
    subject(:strategy) { described_class.new(config) }

    before { config.priority_levels = 3 }

    describe "#physical_queue_names" do
      it "returns sub-queue names for each priority level" do
        expect(strategy.physical_queue_names("default")).to eq(
          %w[pgbus_default_p0 pgbus_default_p1 pgbus_default_p2]
        )
      end
    end

    describe "#target_queue" do
      it "routes to the correct sub-queue by priority" do
        expect(strategy.target_queue("default", 0)).to eq("pgbus_default_p0")
        expect(strategy.target_queue("default", 2)).to eq("pgbus_default_p2")
      end

      it "uses default_priority when priority is nil" do
        config.default_priority = 1
        expect(strategy.target_queue("default", nil)).to eq("pgbus_default_p1")
      end

      it "clamps priority to valid range" do
        expect(strategy.target_queue("default", 99)).to eq("pgbus_default_p2")
        expect(strategy.target_queue("default", -1)).to eq("pgbus_default_p0")
      end
    end

    describe "#priority?" do
      it "returns true" do
        expect(strategy.priority?).to be true
      end
    end
  end
end
