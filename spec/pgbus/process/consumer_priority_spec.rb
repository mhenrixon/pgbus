# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::ConsumerPriority do
  let(:connection) { double("connection") }

  before do
    described_class.reset_cache!
    allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
  end

  after { described_class.reset_cache! }

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

  describe ".max_active_priority caching" do
    let(:rows) { [{ "metadata" => '{"queues":["default"],"consumer_priority":10}' }] }

    before { allow(connection).to receive(:select_all).and_return(rows) }

    it "executes exactly one query for repeated calls within the TTL" do
      described_class.max_active_priority(%w[default], 1)
      described_class.max_active_priority(%w[default], 1)

      expect(connection).to have_received(:select_all).once
    end

    it "returns the same cached value on repeated calls within the TTL" do
      first = described_class.max_active_priority(%w[default], 1)
      second = described_class.max_active_priority(%w[default], 1)

      expect(first).to eq(10)
      expect(second).to eq(10)
    end

    it "re-queries after the TTL expires" do
      described_class.max_active_priority(%w[default], 1)

      allow(Process).to receive(:clock_gettime)
        .with(Process::CLOCK_MONOTONIC)
        .and_return(Process.clock_gettime(Process::CLOCK_MONOTONIC) + described_class::CACHE_TTL + 1)

      described_class.max_active_priority(%w[default], 1)

      expect(connection).to have_received(:select_all).twice
    end

    it "caches different queue lists independently" do
      described_class.max_active_priority(%w[default], 1)
      described_class.max_active_priority(%w[events], 1)

      expect(connection).to have_received(:select_all).twice
    end

    it "caches different pids independently" do
      described_class.max_active_priority(%w[default], 1)
      described_class.max_active_priority(%w[default], 2)

      expect(connection).to have_received(:select_all).twice
    end

    it "treats queue lists as order-independent" do
      described_class.max_active_priority(%w[default events], 1)
      described_class.max_active_priority(%w[events default], 1)

      expect(connection).to have_received(:select_all).once
    end

    it "clears all cached entries with reset_cache!" do
      described_class.max_active_priority(%w[default], 1)
      described_class.reset_cache!
      described_class.max_active_priority(%w[default], 1)

      expect(connection).to have_received(:select_all).twice
    end

    it "consults the cached value from should_yield?" do
      described_class.max_active_priority(%w[default], 1)
      result = described_class.should_yield?(queues: %w[default], my_priority: 0, my_pid: 1)

      expect(result).to be true
      expect(connection).to have_received(:select_all).once
    end
  end
end
