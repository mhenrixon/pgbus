# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::JobContext do
  describe ".from_payload" do
    let(:encoded_payload) do
      {
        "pgbus_current" => {
          "Current" => {
            "tenant" => { "_aj_globalid" => "gid://app/Tenant/42" },
            "request_id" => "req-1",
            "locale" => { "_aj_serialized" => "ActiveJob::Serializers::SymbolSerializer", "value" => "sv" },
            "started_at" => { "_aj_serialized" => "ActiveJob::Serializers::TimeSerializer", "value" => "2026-08-23T10:00:00Z" },
            "flags" => [1, { "_aj_globalid" => "gid://app/Flag/1" }],
            "_aj_symbol_keys" => %w[tenant request_id locale started_at flags]
          },
          "Admin::Current" => { "admin_id" => 7, "_aj_symbol_keys" => ["admin_id"] }
        }
      }
    end

    it "returns nil when the payload has no persisted context" do
      expect(described_class.from_payload({ "job_class" => "X" })).to be_nil
      expect(described_class.from_payload(nil)).to be_nil
      expect(described_class.from_payload({ "pgbus_current" => {} })).to be_nil
    end

    it "unwraps ActiveJob argument encodings into display values per class" do
      context = described_class.from_payload(encoded_payload)

      expect(context.keys).to eq(["Current", "Admin::Current"])
      expect(context["Current"]).to eq(
        "tenant" => "gid://app/Tenant/42",
        "request_id" => "req-1",
        "locale" => "sv",
        "started_at" => "2026-08-23T10:00:00Z",
        "flags" => "[1,\"gid://app/Flag/1\"]"
      )
      expect(context["Admin::Current"]).to eq("admin_id" => "7")
    end

    it "keeps already-filtered values and nested hashes readable" do
      payload = { "pgbus_current" => { "Current" => { "token" => "[FILTERED]", "meta" => { "a" => 1, "_aj_symbol_keys" => ["a"] } } } }

      context = described_class.from_payload(payload)

      expect(context["Current"]).to eq("token" => "[FILTERED]", "meta" => "{\"a\":1}")
    end

    it "accepts a JSON string payload" do
      json = JSON.generate("pgbus_current" => { "Current" => { "tenant" => "acme" } })

      expect(described_class.from_payload(json)).to eq("Current" => { "tenant" => "acme" })
    end

    it "returns nil for a malformed payload string" do
      expect(described_class.from_payload("not json")).to be_nil
    end
  end
end
