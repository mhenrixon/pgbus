# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "defaults streams_default_broadcast_mode to :ephemeral" do
      expect(config.streams_default_broadcast_mode).to eq(:ephemeral)
    end

    it "defaults streams_orphan_sweep_interval to 3600 (1 hour)" do
      expect(config.streams_orphan_sweep_interval).to eq(3600)
    end

    it "defaults streams_orphan_threshold to 86400 (24 hours)" do
      expect(config.streams_orphan_threshold).to eq(86_400)
    end

    it "defaults streams_broadcast_queue to nil (Turbo broadcast jobs share the default queue)" do
      expect(config.streams_broadcast_queue).to be_nil
    end
  end

  describe "#streams_broadcast_queue" do
    it "accepts a queue-name String" do
      config.streams_broadcast_queue = "realtime"
      expect(config.streams_broadcast_queue).to eq("realtime")
    end

    it "accepts nil (default: no dedicated queue)" do
      config.streams_broadcast_queue = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an empty string" do
      config.streams_broadcast_queue = ""
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_broadcast_queue/)
    end

    it "rejects a non-string, non-nil value" do
      config.streams_broadcast_queue = 42
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_broadcast_queue/)
    end
  end

  describe "#streams_default_broadcast_mode=" do
    it "accepts :ephemeral" do
      config.streams_default_broadcast_mode = :ephemeral
      expect(config.streams_default_broadcast_mode).to eq(:ephemeral)
    end

    it "accepts :durable" do
      config.streams_default_broadcast_mode = :durable
      expect(config.streams_default_broadcast_mode).to eq(:durable)
    end

    it "accepts string values and coerces to symbol" do
      config.streams_default_broadcast_mode = "durable"
      expect(config.streams_default_broadcast_mode).to eq(:durable)
    end

    it "rejects invalid values" do
      expect { config.streams_default_broadcast_mode = :bogus }
        .to raise_error(Pgbus::ConfigurationError, /streams_default_broadcast_mode/)
    end
  end

  describe "validation" do
    it "accepts valid streams broadcast config" do
      config.streams_default_broadcast_mode = :durable
      expect { config.validate! }.not_to raise_error
    end

    it "rejects non-positive streams_orphan_sweep_interval" do
      config.streams_orphan_sweep_interval = 0
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_orphan_sweep_interval/)
    end

    it "rejects non-positive streams_orphan_threshold" do
      config.streams_orphan_threshold = -1
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_orphan_threshold/)
    end

    it "accepts nil streams_orphan_sweep_interval (disables sweeper)" do
      config.streams_orphan_sweep_interval = nil
      expect { config.validate! }.not_to raise_error
    end

    it "accepts nil streams_orphan_threshold (disables sweeper)" do
      config.streams_orphan_threshold = nil
      expect { config.validate! }.not_to raise_error
    end
  end
end
