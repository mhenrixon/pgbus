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

  describe ".redact" do
    it "replaces payload-bearing keys with the redaction marker by default" do
      result = described_class.redact(row)

      expect(result[:message]).to eq(described_class::REDACTED)
      expect(result[:headers]).to eq(described_class::REDACTED)
    end

    it "preserves non-payload metadata" do
      result = described_class.redact(row)

      expect(result[:msg_id]).to eq(42)
      expect(result[:read_ct]).to eq(1)
      expect(result[:queue_name]).to eq("pgbus_default")
    end

    it "returns the hash untouched when payloads are allowed" do
      result = described_class.redact(row, include_payloads: true)

      expect(result[:message]).to eq("{\"secret\":\"hunter2\"}")
      expect(result[:headers]).to eq("{\"trace\":\"abc\"}")
    end

    it "leaves nil payload values as nil rather than the marker" do
      result = described_class.redact(row.merge(headers: nil))

      expect(result[:headers]).to be_nil
    end

    it "redacts string-keyed payloads too" do
      result = described_class.redact({ "message" => "x", "msg_id" => 1 })

      expect(result["message"]).to eq(described_class::REDACTED)
      expect(result["msg_id"]).to eq(1)
    end

    it "passes non-hash arguments through unchanged" do
      expect(described_class.redact("plain")).to eq("plain")
    end

    it "does not mutate the input hash" do
      described_class.redact(row)

      expect(row[:message]).to eq("{\"secret\":\"hunter2\"}")
    end
  end

  describe ".redact_all" do
    it "redacts every row in the collection" do
      result = described_class.redact_all([row, row])

      expect(result.map { |r| r[:message] }).to all(eq(described_class::REDACTED))
    end

    it "returns rows untouched when payloads are allowed" do
      result = described_class.redact_all([row], include_payloads: true)

      expect(result.first[:message]).to eq("{\"secret\":\"hunter2\"}")
    end
  end
end
