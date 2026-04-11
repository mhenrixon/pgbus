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
      long_name = "a" * (described_class::MAX_QUEUE_NAME_LENGTH + 1)
      expect { described_class.validate!(long_name) }.to raise_error(ArgumentError, /too long/)
    end

    it "accepts names at max length" do
      name = "a" * described_class::MAX_QUEUE_NAME_LENGTH
      expect(described_class.validate!(name)).to eq(name)
    end

    it "matches the effective limit enforced by pgmq-ruby (< 48 chars)" do
      # pgmq-ruby rejects length >= 48 in PGMQ::Client#validate_queue_name!,
      # so our constant must stay below that to avoid letting names through
      # pgbus that then fail deep in the transport gem.
      expect(described_class::MAX_QUEUE_NAME_LENGTH).to be < 48
    end
  end

  describe ".normalize" do
    it "replaces hyphens with underscores" do
      expect(described_class.normalize("hotwire-livereload")).to eq("hotwire_livereload")
    end

    it "replaces dots with underscores" do
      expect(described_class.normalize("my.queue.name")).to eq("my_queue_name")
    end

    it "collapses consecutive underscores after replacement" do
      expect(described_class.normalize("my--queue")).to eq("my_queue")
    end

    it "strips leading/trailing underscores after replacement" do
      expect(described_class.normalize("-leading")).to eq("leading")
      expect(described_class.normalize("trailing-")).to eq("trailing")
    end

    it "strips characters that are not alphanumeric, hyphens, dots, or underscores" do
      expect(described_class.normalize("queue!@#name")).to eq("queuename")
    end

    it "passes through already-valid names unchanged" do
      expect(described_class.normalize("pgbus_default")).to eq("pgbus_default")
    end

    it "raises when normalized result is blank" do
      expect { described_class.normalize("---!!!") }.to raise_error(ArgumentError, /cannot be blank/)
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
