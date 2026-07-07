# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#doctor_on_boot" do
    it "defaults to nil (off)" do
      expect(config.doctor_on_boot).to be_nil
    end

    it "accepts :report" do
      config.doctor_on_boot = :report
      expect(config.doctor_on_boot).to eq(:report)
    end

    it "accepts :strict" do
      config.doctor_on_boot = :strict
      expect(config.doctor_on_boot).to eq(:strict)
    end

    it "coerces the string \"report\" to the symbol" do
      config.doctor_on_boot = "report"
      expect(config.doctor_on_boot).to eq(:report)
    end

    it "coerces the string \"strict\" to the symbol" do
      config.doctor_on_boot = "strict"
      expect(config.doctor_on_boot).to eq(:strict)
    end

    it "normalizes false to nil (off)" do
      config.doctor_on_boot = false
      expect(config.doctor_on_boot).to be_nil
    end

    it "accepts nil (off)" do
      config.doctor_on_boot = :report
      config.doctor_on_boot = nil
      expect(config.doctor_on_boot).to be_nil
    end

    it "rejects an unknown symbol" do
      expect { config.doctor_on_boot = :loud }
        .to raise_error(Pgbus::ConfigurationError, /doctor_on_boot/)
    end

    it "rejects a non-symbol/non-string/non-nil type" do
      expect { config.doctor_on_boot = 42 }
        .to raise_error(Pgbus::ConfigurationError, /doctor_on_boot/)
    end
  end
end
