# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::WakeSignal do
  subject(:signal) { described_class.new }

  describe "#wait" do
    it "returns false on timeout" do
      result = signal.wait(timeout: 0.01)
      expect(result).to be false
    end

    it "returns true when notified before timeout" do
      Thread.new do
        sleep 0.01
        signal.notify!
      end
      result = signal.wait(timeout: 1)
      expect(result).to be true
    end
  end

  describe "#notify!" do
    it "wakes a waiting thread" do
      woken = false
      thread = Thread.new do
        signal.wait(timeout: 5)
        woken = true
      end
      sleep 0.01 # let thread start waiting
      signal.notify!
      thread.join(1)
      expect(woken).to be true
    end
  end

  describe "#pending?" do
    it "returns false by default" do
      expect(signal.pending?).to be false
    end

    it "returns true after notify" do
      signal.notify!
      expect(signal.pending?).to be true
    end
  end

  describe "#reset!" do
    it "clears pending notification" do
      signal.notify!
      signal.reset!
      expect(signal.pending?).to be false
    end
  end
end
