# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Concurrency::Semaphore do
  describe ".acquire" do
    it "returns :acquired when a slot is available" do
      allow(Pgbus::SemaphoreRecord).to receive(:acquire!).and_return(:acquired)

      expect(described_class.acquire("TestJob-42", 2, 900)).to eq(:acquired)
      expect(Pgbus::SemaphoreRecord).to have_received(:acquire!).with("TestJob-42", 2, an_instance_of(Time))
    end

    it "returns :blocked when limit is reached" do
      allow(Pgbus::SemaphoreRecord).to receive(:acquire!).and_return(:blocked)

      expect(described_class.acquire("TestJob-42", 1, 900)).to eq(:blocked)
    end
  end

  describe ".release" do
    it "decrements the semaphore value" do
      scope = double("scope", update_all: 1)
      allow(Pgbus::SemaphoreRecord).to receive(:where).with(key: "TestJob-42").and_return(scope)

      described_class.release("TestJob-42")

      expect(scope).to have_received(:update_all).with("value = GREATEST(value - 1, 0)")
    end
  end

  describe ".expire_stale" do
    it "deletes expired semaphores and returns their keys" do
      result = double("result", rows: [["old-key-1"], ["old-key-2"]])
      connection = double("connection")
      allow(Pgbus::SemaphoreRecord).to receive(:connection).and_return(connection)
      allow(connection).to receive(:exec_query).and_return(result)

      expired = described_class.expire_stale

      expect(expired).to eq([{ "key" => "old-key-1" }, { "key" => "old-key-2" }])
    end
  end

  describe ".current_value" do
    it "returns the current value for a key" do
      allow(Pgbus::SemaphoreRecord).to receive_message_chain(:where, :pick).and_return(3) # rubocop:disable RSpec/MessageChain

      expect(described_class.current_value("TestJob-42")).to eq(3)
    end

    it "returns nil when key does not exist" do
      allow(Pgbus::SemaphoreRecord).to receive_message_chain(:where, :pick).and_return(nil) # rubocop:disable RSpec/MessageChain

      expect(described_class.current_value("missing")).to be_nil
    end
  end
end
