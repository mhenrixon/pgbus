# frozen_string_literal: true

require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Redactor do
  let(:row) do
    {
      msg_id: 42,
      read_ct: 1,
      queue_name: "pgbus_default",
      message: "{\"secret\":\"hunter2\"}",
      headers: "{\"trace\":\"abc\"}"
    }
  end

  describe ".payload_key?" do
    it "recognizes payload-bearing keys as symbols or strings" do
      expect(described_class.payload_key?(:message)).to be(true)
      expect(described_class.payload_key?("headers")).to be(true)
      expect(described_class.payload_key?(:payload)).to be(true)
      expect(described_class.payload_key?(:arguments)).to be(true)
    end

    it "rejects non-payload keys and never raises on odd keys" do
      expect(described_class.payload_key?(:msg_id)).to be(false)
      expect(described_class.payload_key?(42)).to be(false)
    end
  end

  describe ".deep_redact" do
    it "redacts payload-bearing keys with the marker by default" do
      result = described_class.deep_redact(row)

      expect(result[:message]).to eq(described_class::REDACTED)
      expect(result[:headers]).to eq(described_class::REDACTED)
    end

    it "preserves non-payload metadata" do
      result = described_class.deep_redact(row)

      expect(result[:msg_id]).to eq(42)
      expect(result[:read_ct]).to eq(1)
      expect(result[:queue_name]).to eq("pgbus_default")
    end

    it "leaves nil payload values as nil rather than the marker" do
      result = described_class.deep_redact(row.merge(headers: nil))

      expect(result[:headers]).to be_nil
    end

    it "redacts string-keyed payloads too" do
      result = described_class.deep_redact({ "message" => "x", "msg_id" => 1 })

      expect(result["message"]).to eq(described_class::REDACTED)
      expect(result["msg_id"]).to eq(1)
    end

    it "does not mutate the input hash" do
      described_class.deep_redact(row)

      expect(row[:message]).to eq("{\"secret\":\"hunter2\"}")
    end

    it "redacts payload keys nested inside an envelope hash" do
      result = described_class.deep_redact({ job: row })

      expect(result[:job][:message]).to eq(described_class::REDACTED)
      expect(result[:job][:msg_id]).to eq(42)
    end

    it "redacts payload keys inside an array of rows under an envelope key" do
      result = described_class.deep_redact({ jobs: [row, row] })

      expect(result[:jobs].map { |r| r[:message] }).to all(eq(described_class::REDACTED))
    end

    it "descends into an envelope key that shares a payload name instead of blanking it" do
      # {message: {...}} is a detail wrapper, not a raw body — recurse, then
      # redact the leaf :message inside.
      result = described_class.deep_redact({ message: row })

      expect(result[:message]).to be_a(Hash)
      expect(result[:message][:message]).to eq(described_class::REDACTED)
      expect(result[:message][:msg_id]).to eq(42)
    end

    it "returns the structure untouched when payloads are allowed" do
      result = described_class.deep_redact({ jobs: [row] }, include_payloads: true)

      expect(result[:jobs].first[:message]).to eq("{\"secret\":\"hunter2\"}")
    end

    it "passes scalars through" do
      expect(described_class.deep_redact("OK")).to eq("OK")
      expect(described_class.deep_redact(42)).to eq(42)
      expect(described_class.deep_redact(nil)).to be_nil
    end
  end
end
