# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Concurrency::Semaphore do
  let(:connection) { double("AR::Connection") }

  before do
    stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
  end

  describe ".acquire" do
    it "returns :acquired when a slot is available" do
      result = double("Result", any?: true)
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.acquire("TestJob-42", 2, 900)).to eq(:acquired)
      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/INSERT INTO pgbus_semaphores/),
        "Pgbus Semaphore Acquire",
        array_including("TestJob-42", 2, an_instance_of(Time))
      )
    end

    it "returns :blocked when limit is reached" do
      result = double("Result", any?: false)
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.acquire("TestJob-42", 1, 900)).to eq(:blocked)
    end
  end

  describe ".release" do
    it "decrements the semaphore value" do
      allow(connection).to receive(:exec_query).and_return([])

      described_class.release("TestJob-42")
      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/UPDATE pgbus_semaphores/),
        "Pgbus Semaphore Release",
        ["TestJob-42"]
      )
    end
  end

  describe ".expire_stale" do
    it "deletes expired semaphores and returns their keys" do
      rows = [{ "key" => "old-key-1" }, { "key" => "old-key-2" }]
      result = double("Result", to_a: rows)
      allow(connection).to receive(:exec_query).and_return(result)

      expired = described_class.expire_stale
      expect(expired.size).to eq(2)
    end
  end

  describe ".current_value" do
    it "returns the current value for a key" do
      result = double("Result", first: { "value" => "3" })
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.current_value("TestJob-42")).to eq(3)
    end

    it "returns nil when key does not exist" do
      result = double("Result", first: nil)
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.current_value("missing")).to be_nil
    end
  end

  context "when ActiveRecord is not defined" do
    before { hide_const("ActiveRecord") }

    it "acquire returns :blocked" do
      expect(described_class.acquire("key", 1, 900)).to eq(:blocked)
    end

    it "release returns empty" do
      expect(described_class.release("key")).to eq([])
    end
  end
end
