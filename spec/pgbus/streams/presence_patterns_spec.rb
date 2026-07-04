# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  describe "#streams_presence_patterns" do
    subject(:config) { described_class.new }

    it "defaults to an empty array" do
      expect(config.streams_presence_patterns).to eq([])
    end

    it "rejects non-Array values" do
      config.streams_presence_patterns = "chat"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_presence_patterns/)
    end

    it "accepts an array of strings and regex" do
      config.streams_presence_patterns = ["chat", /^rooms:/]
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "#streams_presence_member validation" do
    subject(:config) { described_class.new }

    it "accepts nil (uses the built-in extractor)" do
      config.streams_presence_member = nil
      expect { config.validate! }.not_to raise_error
    end

    it "accepts a callable (Proc/lambda)" do
      config.streams_presence_member = ->(ctx) { { id: ctx[:id] } }
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-callable value at boot (fail fast)" do
      config.streams_presence_member = "not callable"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_presence_member must respond to #call/)
    end
  end

  describe "#stream_presence?" do
    subject(:config) { described_class.new }

    it "is false by default (no patterns)" do
      expect(config.stream_presence?("anything")).to be(false)
    end

    it "matches an exact string pattern" do
      config.streams_presence_patterns = ["chat"]
      expect(config.stream_presence?("chat")).to be(true)
      expect(config.stream_presence?("other")).to be(false)
    end

    it "matches a regex pattern" do
      config.streams_presence_patterns = [/^rooms:/]
      expect(config.stream_presence?("rooms:42")).to be(true)
      expect(config.stream_presence?("chat:42")).to be(false)
    end
  end

  describe "#presence_member_for" do
    subject(:config) { described_class.new }

    it "returns nil for a nil context (anonymous connection)" do
      expect(config.presence_member_for(nil)).to be_nil
    end

    it "extracts id and metadata from a Hash context with :member_id" do
      result = config.presence_member_for({ member_id: "7", metadata: { name: "Ann" } })
      expect(result).to eq({ id: "7", metadata: { name: "Ann" } })
    end

    it "extracts id from a Hash context with :id when :member_id is absent" do
      result = config.presence_member_for({ id: 9 })
      expect(result).to eq({ id: "9", metadata: {} })
    end

    it "extracts id from an object responding to #id (e.g. a user model)" do
      user = Struct.new(:id).new(42)
      result = config.presence_member_for(user)
      expect(result).to eq({ id: "42", metadata: {} })
    end

    it "returns nil when no id can be derived" do
      expect(config.presence_member_for(Object.new)).to be_nil
    end

    it "uses a custom streams_presence_member callable when configured" do
      config.streams_presence_member = lambda do |ctx|
        { id: ctx[:uid], metadata: { role: ctx[:role] } }
      end
      result = config.presence_member_for({ uid: "u1", role: "admin" })
      expect(result).to eq({ id: "u1", metadata: { role: "admin" } })
    end

    it "returns nil when the custom callable yields a blank id" do
      config.streams_presence_member = ->(_ctx) { { id: nil } }
      expect(config.presence_member_for({})).to be_nil
    end

    it "coerces the custom callable's id to a String and defaults metadata to {}" do
      config.streams_presence_member = ->(_ctx) { { id: 5 } }
      expect(config.presence_member_for({})).to eq({ id: "5", metadata: {} })
    end
  end
end
