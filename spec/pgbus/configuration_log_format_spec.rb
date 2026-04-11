# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#log_format" do
    it "defaults to :text" do
      expect(config.log_format).to eq(:text)
    end

    it "can be set to :json" do
      config.log_format = :json

      expect(config.log_format).to eq(:json)
      expect(config.logger.formatter).to be_a(Pgbus::LogFormatter::JSON)
    end

    it "can be set to :text" do
      config.log_format = :json
      config.log_format = :text

      expect(config.log_format).to eq(:text)
      expect(config.logger.formatter).to be_a(Pgbus::LogFormatter::Text)
    end

    it "raises for invalid formats" do
      expect { config.log_format = :xml }.to raise_error(ArgumentError, /Invalid log_format/)
    end

    it "accepts string values" do
      config.log_format = "json"

      expect(config.log_format).to eq(:json)
    end
  end
end
