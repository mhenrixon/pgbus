# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus do
  it "has a version number" do
    expect(Pgbus::VERSION).not_to be_nil
  end

  describe ".configuration" do
    it "returns a Configuration instance" do
      expect(described_class.configuration).to be_a(Pgbus::Configuration)
    end

    it "memoizes the configuration" do
      expect(described_class.configuration).to be(described_class.configuration)
    end
  end

  describe ".configure" do
    it "yields the configuration" do
      described_class.configure do |config|
        expect(config).to be_a(Pgbus::Configuration)
      end
    end
  end

  describe ".logger" do
    it "reads from configuration" do
      expect(described_class.logger).to eq(described_class.configuration.logger)
    end
  end

  describe ".logger=" do
    it "writes to configuration" do
      custom_logger = Logger.new(IO::NULL)
      original = described_class.configuration.logger

      described_class.logger = custom_logger
      expect(described_class.configuration.logger).to be(custom_logger)
    ensure
      described_class.configuration.logger = original
    end
  end

  describe ".reset!" do
    it "clears configuration" do
      old_config = described_class.configuration
      described_class.reset!
      expect(described_class.configuration).not_to be(old_config)
    end
  end
end
