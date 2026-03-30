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

  describe ".reset!" do
    it "clears configuration" do
      old_config = described_class.configuration
      described_class.reset!
      expect(described_class.configuration).not_to be(old_config)
    end
  end
end
