# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DedupCache do
  subject(:cache) { described_class.new(max_size: 5, ttl: 10) }

  describe "#seen?" do
    it "returns false for unseen keys" do
      expect(cache.seen?("event-1:Handler")).to be false
    end

    it "returns true for recently marked keys" do
      cache.mark!("event-1:Handler")
      expect(cache.seen?("event-1:Handler")).to be true
    end

    it "returns false for expired keys" do
      cache.mark!("event-1:Handler")
      # Simulate TTL expiry by backdating the entry
      cache.instance_variable_get(:@cache)["event-1:Handler"] =
        Process.clock_gettime(Process::CLOCK_MONOTONIC) - 20
      expect(cache.seen?("event-1:Handler")).to be false
    end
  end

  describe "#mark!" do
    it "adds the key to the cache" do
      cache.mark!("event-1:Handler")
      expect(cache.size).to eq(1)
    end

    it "evicts the oldest entry when max_size is reached" do
      5.times { |i| cache.mark!("event-#{i}:Handler") }
      expect(cache.size).to eq(5)

      cache.mark!("event-new:Handler")
      expect(cache.size).to eq(5)
      # oldest entry should be gone
      expect(cache.seen?("event-0:Handler")).to be false
      # newest should still be there
      expect(cache.seen?("event-new:Handler")).to be true
    end
  end

  describe "#clear!" do
    it "removes all entries" do
      3.times { |i| cache.mark!("event-#{i}:Handler") }
      cache.clear!
      expect(cache.size).to eq(0)
    end
  end

  describe "thread safety" do
    it "handles concurrent access without errors" do
      big_cache = described_class.new(max_size: 1000, ttl: 60)
      threads = 10.times.map do |t|
        Thread.new do
          100.times do |i|
            key = "event-#{t}-#{i}:Handler"
            big_cache.mark!(key)
            big_cache.seen?(key)
          end
        end
      end
      threads.each(&:join)
      expect(big_cache.size).to be <= 1000
    end
  end
end
