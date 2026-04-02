# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::QueueNameValidator do
  describe ".validate!" do
    it "accepts valid alphanumeric queue names" do
      expect(described_class.validate!("pgbus_default")).to eq("pgbus_default")
    end

    it "accepts names with numbers" do
      expect(described_class.validate!("queue_42_p0")).to eq("queue_42_p0")
    end

    it "rejects blank names" do
      expect { described_class.validate!("") }.to raise_error(ArgumentError, /cannot be blank/)
    end

    it "rejects names with special characters" do
      expect { described_class.validate!("queue; DROP TABLE users--") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with SQL injection attempts" do
      expect { described_class.validate!("q' OR '1'='1") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with dots" do
      expect { described_class.validate!("pgmq.q_default") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names with dashes" do
      expect { described_class.validate!("my-queue") }.to raise_error(ArgumentError, /Only alphanumeric/)
    end

    it "rejects names exceeding max length" do
      long_name = "a" * 62
      expect { described_class.validate!(long_name) }.to raise_error(ArgumentError, /too long/)
    end

    it "accepts names at max length" do
      name = "a" * 61
      expect(described_class.validate!(name)).to eq(name)
    end
  end

  describe ".sanitize!" do
    it "strips invalid characters and returns the result" do
      expect(described_class.sanitize!("my-queue.name!")).to eq("myqueuename")
    end

    it "raises when sanitized result is blank" do
      expect { described_class.sanitize!("---!!!") }.to raise_error(ArgumentError, /cannot be blank/)
    end

    it "strips SQL injection attempts" do
      result = described_class.sanitize!("queue'; DROP TABLE users--")
      expect(result).to eq("queueDROPTABLEusers")
    end
  end
end
