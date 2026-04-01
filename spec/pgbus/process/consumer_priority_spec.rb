# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::ConsumerPriority do
  let(:connection) { double("connection") }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
  end

  describe ".should_yield?" do
    it "returns false when no other workers exist" do
      allow(connection).to receive(:select_all).and_return([])

      result = described_class.should_yield?(queues: %w[default], my_priority: 0, my_pid: 1)
      expect(result).to be false
    end

    it "returns true when a higher-priority worker shares queues" do
      rows = [{ "metadata" => '{"queues":["default"],"consumer_priority":10}' }]
      allow(connection).to receive(:select_all).and_return(rows)

      result = described_class.should_yield?(queues: %w[default], my_priority: 0, my_pid: 1)
      expect(result).to be true
    end

    it "returns false when this worker has the highest priority" do
      rows = [{ "metadata" => '{"queues":["default"],"consumer_priority":5}' }]
      allow(connection).to receive(:select_all).and_return(rows)

      result = described_class.should_yield?(queues: %w[default], my_priority: 10, my_pid: 1)
      expect(result).to be false
    end

    it "returns false when higher-priority workers are on different queues" do
      rows = [{ "metadata" => '{"queues":["events"],"consumer_priority":10}' }]
      allow(connection).to receive(:select_all).and_return(rows)

      result = described_class.should_yield?(queues: %w[default], my_priority: 0, my_pid: 1)
      expect(result).to be false
    end

    it "handles errors gracefully" do
      allow(connection).to receive(:select_all).and_raise(StandardError, "db error")

      result = described_class.should_yield?(queues: %w[default], my_priority: 0, my_pid: 1)
      expect(result).to be false
    end
  end

  describe ".effective_polling_interval" do
    it "returns base interval for highest-priority worker" do
      result = described_class.effective_polling_interval(
        base_interval: 0.1, my_priority: 10, max_priority: 5
      )
      expect(result).to eq(0.1)
    end

    it "returns 3x interval for lower-priority workers" do
      result = described_class.effective_polling_interval(
        base_interval: 0.1, my_priority: 0, max_priority: 10
      )
      expect(result).to be_within(0.01).of(0.3)
    end

    it "returns base interval when priorities are equal" do
      result = described_class.effective_polling_interval(
        base_interval: 0.1, my_priority: 5, max_priority: 5
      )
      expect(result).to eq(0.1)
    end
  end
end
