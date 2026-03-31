# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#pgmq_schema_mode" do
    it "defaults to :auto" do
      expect(config.pgmq_schema_mode).to eq(:auto)
    end

    it "accepts :extension" do
      config.pgmq_schema_mode = :extension
      expect(config.pgmq_schema_mode).to eq(:extension)
    end

    it "accepts :embedded" do
      config.pgmq_schema_mode = :embedded
      expect(config.pgmq_schema_mode).to eq(:embedded)
    end

    it "rejects invalid modes" do
      expect { config.pgmq_schema_mode = :invalid }
        .to raise_error(ArgumentError, /invalid/)
    end
  end
end
