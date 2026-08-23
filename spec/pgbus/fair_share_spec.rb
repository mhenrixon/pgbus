# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::FairShare do
  let(:config) { Pgbus::Configuration.new }
  let(:job) { build_job_double(job_class: "TestJob") }
  let(:payload) { { "job_class" => "TestJob", "arguments" => [] } }

  describe ".enabled?" do
    it "is false when config.fair_share is nil" do
      expect(described_class.enabled?(config)).to be(false)
    end

    it "is true when config.fair_share is set" do
      config.fair_share = ->(_job) { "t" }
      expect(described_class.enabled?(config)).to be(true)
    end
  end

  describe ".inject_metadata" do
    it "returns the payload untouched when fair share is disabled" do
      expect(described_class.inject_metadata(job, payload, config)).to equal(payload)
    end

    it "returns the payload untouched when the callable returns nil" do
      config.fair_share = ->(_job) {}
      result = described_class.inject_metadata(job, payload, config)
      expect(result).to eq(payload)
      expect(result).not_to have_key("pgbus_fair_key")
    end

    it "passes the job to the callable" do
      seen = nil
      config.fair_share = lambda { |j|
        seen = j
        "t"
      }
      described_class.inject_metadata(job, payload, config)
      expect(seen).to equal(job)
    end

    it "stores a string key and no weight when only a key is returned" do
      config.fair_share = ->(_job) { "tenant-42" }
      result = described_class.inject_metadata(job, payload, config)
      expect(result["pgbus_fair_key"]).to eq("tenant-42")
      expect(result).not_to have_key("pgbus_fair_weight")
    end

    it "does not mutate the original payload" do
      config.fair_share = ->(_job) { "tenant-42" }
      described_class.inject_metadata(job, payload, config)
      expect(payload).not_to have_key("pgbus_fair_key")
    end

    it "stringifies Integer and Symbol keys" do
      config.fair_share = ->(_job) { 42 }
      expect(described_class.inject_metadata(job, payload, config)["pgbus_fair_key"]).to eq("42")

      config.fair_share = ->(_job) { :acme }
      expect(described_class.inject_metadata(job, payload, config)["pgbus_fair_key"]).to eq("acme")
    end

    it "stores key and weight when [key, weight] is returned" do
      config.fair_share = ->(_job) { ["tenant-42", 3] }
      result = described_class.inject_metadata(job, payload, config)
      expect(result["pgbus_fair_key"]).to eq("tenant-42")
      expect(result["pgbus_fair_weight"]).to eq(3)
    end

    it "accepts a fractional weight" do
      config.fair_share = ->(_job) { ["tenant-42", 0.5] }
      expect(described_class.inject_metadata(job, payload, config)["pgbus_fair_weight"]).to eq(0.5)
    end

    it "omits the weight key when the weight is 1 (the default)" do
      config.fair_share = ->(_job) { ["tenant-42", 1] }
      result = described_class.inject_metadata(job, payload, config)
      expect(result).not_to have_key("pgbus_fair_weight")
    end

    it "treats [key, nil] as default weight" do
      config.fair_share = ->(_job) { ["tenant-42", nil] }
      result = described_class.inject_metadata(job, payload, config)
      expect(result["pgbus_fair_key"]).to eq("tenant-42")
      expect(result).not_to have_key("pgbus_fair_weight")
    end

    it "raises ArgumentError for a non-positive weight" do
      config.fair_share = ->(_job) { ["tenant-42", 0] }
      expect { described_class.inject_metadata(job, payload, config) }.to raise_error(ArgumentError, /weight/)
    end

    it "raises ArgumentError for a non-numeric weight" do
      config.fair_share = ->(_job) { %w[tenant-42 heavy] }
      expect { described_class.inject_metadata(job, payload, config) }.to raise_error(ArgumentError, /weight/)
    end

    it "raises ArgumentError for an unsupported key type" do
      config.fair_share = ->(_job) { Object.new }
      expect { described_class.inject_metadata(job, payload, config) }.to raise_error(ArgumentError, /key/)
    end

    it "raises ArgumentError for an empty key" do
      config.fair_share = ->(_job) { "" }
      expect { described_class.inject_metadata(job, payload, config) }.to raise_error(ArgumentError, /key/)
    end

    it "lets exceptions raised by the callable propagate" do
      config.fair_share = ->(_job) { raise "no tenant" }
      expect { described_class.inject_metadata(job, payload, config) }.to raise_error(RuntimeError, "no tenant")
    end
  end

  describe ".event_enabled?" do
    it "is false when config.event_fair_share is nil" do
      expect(described_class.event_enabled?(config)).to be(false)
    end

    it "is true when config.event_fair_share is set" do
      config.event_fair_share = ->(_event) { "t" }
      expect(described_class.event_enabled?(config)).to be(true)
    end

    it "is independent of config.fair_share" do
      config.fair_share = ->(_job) { "t" }
      expect(described_class.event_enabled?(config)).to be(false)
      expect(described_class.enabled?(config)).to be(true)
    end
  end

  describe ".inject_event_metadata (issue #427)" do
    let(:event) do
      Pgbus::Event.new(event_id: "evt-1", payload: { "tenant_id" => 7 }, routing_key: "orders.created",
                       headers: { "x" => "y" })
    end
    let(:event_data) do
      { "event_id" => "evt-1", "payload" => { "tenant_id" => 7 }, "published_at" => "2026-01-01T00:00:00Z",
        "routing_key" => "orders.created" }
    end

    it "returns the event_data untouched (same object) when event fair share is disabled" do
      expect(described_class.inject_event_metadata(event, event_data, config)).to equal(event_data)
    end

    it "returns the event_data untouched when the callable returns nil" do
      config.event_fair_share = ->(_event) {}
      result = described_class.inject_event_metadata(event, event_data, config)
      expect(result).to eq(event_data)
      expect(result).not_to have_key("pgbus_fair_key")
    end

    it "passes the Pgbus::Event to the callable" do
      seen = nil
      config.event_fair_share = lambda { |e|
        seen = e
        "t"
      }
      described_class.inject_event_metadata(event, event_data, config)
      expect(seen).to equal(event)
    end

    it "tags the envelope, not the user payload" do
      config.event_fair_share = ->(e) { e.payload["tenant_id"] }
      result = described_class.inject_event_metadata(event, event_data, config)
      expect(result["pgbus_fair_key"]).to eq("7")
      expect(result["payload"]).to eq("tenant_id" => 7)
      expect(result).not_to have_key("pgbus_fair_weight")
    end

    it "does not mutate the original event_data" do
      config.event_fair_share = ->(_e) { "t" }
      described_class.inject_event_metadata(event, event_data, config)
      expect(event_data).not_to have_key("pgbus_fair_key")
    end

    it "stores key and weight when [key, weight] is returned" do
      config.event_fair_share = ->(e) { [e.routing_key, 3] }
      result = described_class.inject_event_metadata(event, event_data, config)
      expect(result["pgbus_fair_key"]).to eq("orders.created")
      expect(result["pgbus_fair_weight"]).to eq(3)
    end

    it "applies the same normalization as the job side" do
      config.event_fair_share = ->(_e) { ["", 1] }
      expect { described_class.inject_event_metadata(event, event_data, config) }.to raise_error(ArgumentError, /key/)

      config.event_fair_share = ->(_e) { ["t", -1] }
      expect { described_class.inject_event_metadata(event, event_data, config) }.to raise_error(ArgumentError, /weight/)
    end

    it "lets exceptions raised by the callable propagate" do
      config.event_fair_share = ->(_e) { raise "no tenant" }
      expect { described_class.inject_event_metadata(event, event_data, config) }.to raise_error(RuntimeError, "no tenant")
    end
  end

  describe ".extract_key / .extract_weight" do
    it "round-trips the metadata" do
      tagged = payload.merge("pgbus_fair_key" => "t", "pgbus_fair_weight" => 2)
      expect(described_class.extract_key(tagged)).to eq("t")
      expect(described_class.extract_weight(tagged)).to eq(2)
    end

    it "returns nil key and weight 1 for untagged payloads" do
      expect(described_class.extract_key(payload)).to be_nil
      expect(described_class.extract_weight(payload)).to eq(1)
    end
  end
end
