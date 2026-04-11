# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#error_reporters" do
    it "defaults to an empty array" do
      expect(config.error_reporters).to eq([])
    end

    it "accepts callable objects" do
      reporter = ->(ex, ctx) { [ex, ctx] }
      config.error_reporters << reporter

      expect(config.error_reporters).to contain_exactly(reporter)
    end

    it "can be replaced entirely" do
      reporter = ->(ex, ctx) { [ex, ctx] }
      config.error_reporters = [reporter]

      expect(config.error_reporters).to contain_exactly(reporter)
    end
  end
end
