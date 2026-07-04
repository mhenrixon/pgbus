# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  before do
    Pgbus.reset!
    Pgbus.configure do |c|
      c.streams_default_broadcast_mode = :ephemeral
    end
  end

  after { Pgbus.reset! }

  describe "Pgbus.stream(name) without explicit durable: kwarg" do
    it "uses :ephemeral default when no patterns match" do
      stream = Pgbus.stream("anything")
      expect(stream).not_to be_durable
    end

    it "treats stream as durable when name matches an exact streams_durable_patterns entry" do
      Pgbus.configuration.streams_durable_patterns = ["chat"]
      stream = Pgbus.stream("chat")
      expect(stream).to be_durable
    end

    it "treats stream as durable when name matches a regex pattern" do
      Pgbus.configuration.streams_durable_patterns = [/^chat:/]
      stream = Pgbus.stream("chat:42:messages")
      expect(stream).to be_durable
    end

    it "does not affect non-matching streams" do
      Pgbus.configuration.streams_durable_patterns = [/^chat:/]
      expect(Pgbus.stream("notifications:42")).not_to be_durable
    end

    it "respects explicit durable: kwarg over pattern matches" do
      Pgbus.configuration.streams_durable_patterns = [/^chat:/]
      expect(Pgbus.stream("chat:42", durable: false)).not_to be_durable
    end

    it "uses :durable default when streams_default_broadcast_mode is :durable" do
      Pgbus.configuration.streams_default_broadcast_mode = :durable
      Pgbus.configuration.streams_durable_patterns = []
      expect(Pgbus.stream("anything")).to be_durable
    end
  end

  describe "Pgbus::Configuration#streams_durable_patterns" do
    subject(:config) { described_class.new }

    it "defaults to an empty array" do
      expect(config.streams_durable_patterns).to eq([])
    end

    it "rejects non-Array values" do
      config.streams_durable_patterns = "chat"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_durable_patterns/)
    end

    it "accepts an array of strings and regex" do
      config.streams_durable_patterns = ["chat", /^orders:/]
      expect { config.validate! }.not_to raise_error
    end
  end
end
