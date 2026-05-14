# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::StreamCounter do
  subject(:counter) { described_class.new }

  describe "#increment_broadcasts" do
    it "tracks broadcasts per stream" do
      counter.increment_broadcasts("chat")
      counter.increment_broadcasts("chat")
      counter.increment_broadcasts("alerts")

      expect(counter.broadcasts("chat")).to eq(2)
      expect(counter.broadcasts("alerts")).to eq(1)
    end

    it "returns 0 for unknown streams" do
      expect(counter.broadcasts("unknown")).to eq(0)
    end
  end

  describe "#increment_connections / #decrement_connections" do
    it "tracks net active connections per stream" do
      counter.increment_connections("chat")
      counter.increment_connections("chat")
      counter.increment_connections("chat")
      counter.decrement_connections("chat")

      expect(counter.active_connections("chat")).to eq(2)
    end

    it "floors at zero" do
      counter.decrement_connections("chat")
      expect(counter.active_connections("chat")).to eq(0)
    end

    it "returns 0 for unknown streams" do
      expect(counter.active_connections("unknown")).to eq(0)
    end
  end

  describe "#increment_total_connections" do
    it "tracks cumulative connection count per stream" do
      counter.increment_total_connections("chat")
      counter.increment_total_connections("chat")

      expect(counter.total_connections("chat")).to eq(2)
    end
  end

  describe "#snapshot" do
    it "returns per-stream metrics for all known streams" do
      counter.increment_broadcasts("chat")
      counter.increment_broadcasts("chat")
      counter.increment_connections("chat")
      counter.increment_total_connections("chat")
      counter.increment_broadcasts("alerts")

      result = counter.snapshot

      expect(result).to include(
        "chat" => {
          broadcasts: 2,
          active_connections: 1,
          total_connections: 1
        }
      )
      expect(result).to include(
        "alerts" => {
          broadcasts: 1,
          active_connections: 0,
          total_connections: 0
        }
      )
    end

    it "returns an empty hash when no streams have been seen" do
      expect(counter.snapshot).to eq({})
    end
  end

  describe "#totals" do
    it "returns aggregate counts across all streams" do
      counter.increment_broadcasts("chat")
      counter.increment_broadcasts("alerts")
      counter.increment_connections("chat")
      counter.increment_total_connections("chat")

      result = counter.totals

      expect(result).to eq(
        broadcasts: 2,
        active_connections: 1,
        total_connections: 1,
        streams: 2
      )
    end
  end

  describe "thread safety" do
    it "handles concurrent increments without data loss" do
      threads = 10.times.map do
        Thread.new do
          100.times { counter.increment_broadcasts("chat") }
        end
      end
      threads.each(&:join)

      expect(counter.broadcasts("chat")).to eq(1000)
    end
  end
end
