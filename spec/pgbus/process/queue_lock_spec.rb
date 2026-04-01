# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::QueueLock do
  subject(:lock) { described_class.new }

  let(:connection) { double("connection") }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
  end

  describe "#try_lock" do
    it "acquires an advisory lock and returns true" do
      allow(connection).to receive(:select_value).and_return(true)
      expect(lock.try_lock("default")).to be true
    end

    it "returns false when another process holds the lock" do
      allow(connection).to receive(:select_value).and_return(false)
      expect(lock.try_lock("default")).to be false
    end

    it "returns true without re-locking when already held" do
      allow(connection).to receive(:select_value).and_return(true)
      lock.try_lock("default")
      lock.try_lock("default")
      # Should only call select_value once (cached)
      expect(connection).to have_received(:select_value).once
    end

    it "handles errors gracefully" do
      allow(connection).to receive(:select_value).and_raise(StandardError, "db error")
      expect(lock.try_lock("default")).to be false
    end
  end

  describe "#unlock" do
    it "releases a held lock" do
      allow(connection).to receive(:select_value).and_return(true)
      lock.try_lock("default")
      lock.unlock("default")
      expect(connection).to have_received(:select_value).twice # lock + unlock
    end

    it "does nothing for unheld queues" do
      allow(connection).to receive(:select_value)
      lock.unlock("default")
      expect(connection).not_to have_received(:select_value)
    end
  end

  describe "#unlock_all" do
    it "releases all held locks" do
      allow(connection).to receive(:select_value).and_return(true)
      lock.try_lock("default")
      lock.try_lock("events")
      lock.unlock_all
      # 2 locks + 2 unlocks = 4 calls
      expect(connection).to have_received(:select_value).exactly(4).times
    end
  end

  describe "#locked?" do
    it "returns true for held queues" do
      allow(connection).to receive(:select_value).and_return(true)
      lock.try_lock("default")
      expect(lock.locked?("default")).to be true
    end

    it "returns false for unheld queues" do
      expect(lock.locked?("default")).to be false
    end
  end

  describe "#held_queues" do
    it "returns list of locked queue names" do
      allow(connection).to receive(:select_value).and_return(true)
      lock.try_lock("default")
      lock.try_lock("events")
      expect(lock.held_queues).to contain_exactly("default", "events")
    end
  end
end
