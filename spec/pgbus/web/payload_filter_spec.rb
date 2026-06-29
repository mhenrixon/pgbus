# frozen_string_literal: true

require "spec_helper"

require "active_support"
require "active_support/parameter_filter"
require_relative "../../../lib/pgbus/web/payload_filter"

RSpec.describe Pgbus::Web::PayloadFilter do
  after do
    Pgbus.configuration.dashboard_filter_parameters = nil
    Pgbus.configuration.dashboard_filter_sensitive = true
  end

  describe ".filter" do
    context "with default filter patterns" do
      it "redacts password fields" do
        payload = { "job_class" => "UserJob", "arguments" => [{ "password" => "s3cret" }] }
        result = described_class.filter(payload)

        expect(result["arguments"].first["password"]).to eq("[FILTERED]")
      end

      it "redacts token fields" do
        payload = { "token" => "abc123", "user_id" => 42 }
        result = described_class.filter(payload)

        expect(result["token"]).to eq("[FILTERED]")
        expect(result["user_id"]).to eq(42)
      end

      it "redacts secret fields" do
        payload = { "secret" => "mysecret", "name" => "visible" }
        result = described_class.filter(payload)

        expect(result["secret"]).to eq("[FILTERED]")
        expect(result["name"]).to eq("visible")
      end

      it "preserves non-sensitive fields" do
        payload = { "job_class" => "MyJob", "queue_name" => "default", "job_id" => "abc-123" }
        result = described_class.filter(payload)

        expect(result).to eq(payload)
      end
    end

    context "with nested hashes" do
      it "filters deeply nested sensitive keys" do
        payload = {
          "arguments" => [{
            "user" => {
              "name" => "Alice",
              "credentials" => {
                "password" => "deep_secret",
                "api_key" => "key123"
              }
            }
          }]
        }
        result = described_class.filter(payload)

        creds = result["arguments"].first["user"]["credentials"]
        expect(creds["password"]).to eq("[FILTERED]")
        expect(creds["api_key"]).to eq("[FILTERED]")
        expect(result["arguments"].first["user"]["name"]).to eq("Alice")
      end
    end

    context "with arrays" do
      it "filters sensitive keys within array elements" do
        payload = {
          "arguments" => [
            { "password" => "one", "name" => "Alice" },
            { "password" => "two", "name" => "Bob" }
          ]
        }
        result = described_class.filter(payload)

        expect(result["arguments"][0]["password"]).to eq("[FILTERED]")
        expect(result["arguments"][0]["name"]).to eq("Alice")
        expect(result["arguments"][1]["password"]).to eq("[FILTERED]")
        expect(result["arguments"][1]["name"]).to eq("Bob")
      end
    end

    context "with symbol keys" do
      it "filters symbol-keyed hashes" do
        payload = { password: "s3cret", name: "Alice" }
        result = described_class.filter(payload)

        expect(result[:password]).to eq("[FILTERED]")
        expect(result[:name]).to eq("Alice")
      end
    end

    context "with non-hash input" do
      it "returns nil as-is" do
        expect(described_class.filter(nil)).to be_nil
      end

      it "returns strings as-is" do
        expect(described_class.filter("raw string")).to eq("raw string")
      end

      it "filters hashes within arrays" do
        payload = [{ "password" => "s3cret" }, "plain"]
        result = described_class.filter(payload)

        expect(result[0]["password"]).to eq("[FILTERED]")
        expect(result[1]).to eq("plain")
      end
    end

    context "with custom filter patterns" do
      before do
        Pgbus.configuration.dashboard_filter_parameters = [:ssn, /credit_card/]
      end

      it "uses custom patterns" do
        payload = { "ssn" => "123-45-6789", "name" => "Alice" }
        result = described_class.filter(payload)

        expect(result["ssn"]).to eq("[FILTERED]")
        expect(result["name"]).to eq("Alice")
      end

      it "matches regex patterns" do
        payload = { "credit_card_number" => "4111111111111111", "amount" => 99 }
        result = described_class.filter(payload)

        expect(result["credit_card_number"]).to eq("[FILTERED]")
        expect(result["amount"]).to eq(99)
      end
    end

    context "when filtering is disabled" do
      before do
        Pgbus.configuration.dashboard_filter_sensitive = false
      end

      it "returns payload unmodified" do
        payload = { "password" => "s3cret", "token" => "abc123" }
        result = described_class.filter(payload)

        expect(result).to eq(payload)
      end
    end
  end

  describe ".filter_json" do
    it "parses, filters, and re-serializes JSON strings" do
      json = '{"password":"s3cret","name":"Alice"}'
      result = described_class.filter_json(json)

      parsed = JSON.parse(result)
      expect(parsed["password"]).to eq("[FILTERED]")
      expect(parsed["name"]).to eq("Alice")
    end

    it "returns non-JSON strings as-is" do
      expect(described_class.filter_json("not json")).to eq("not json")
    end

    it "returns nil as-is" do
      expect(described_class.filter_json(nil)).to be_nil
    end

    it "handles hash input by filtering and serializing" do
      hash = { "password" => "s3cret", "name" => "Alice" }
      result = described_class.filter_json(hash)

      parsed = JSON.parse(result)
      expect(parsed["password"]).to eq("[FILTERED]")
      expect(parsed["name"]).to eq("Alice")
    end
  end

  describe "default filter patterns" do
    described_class::DEFAULT_FILTER_PATTERNS.each do |pattern|
      it "includes #{pattern.inspect} as a default" do
        expect(described_class::DEFAULT_FILTER_PATTERNS).to include(pattern)
      end
    end

    %w[password token secret authorization api_key private_key].each do |sensitive_key|
      it "filters '#{sensitive_key}' by default" do
        payload = { sensitive_key => "value" }
        result = described_class.filter(payload)

        expect(result[sensitive_key]).to eq("[FILTERED]")
      end
    end
  end

  describe ".rails_filter_parameters" do
    it "returns nil when Rails is not defined with filter_parameters" do
      expect(described_class.send(:rails_filter_parameters)).to be_nil
    end
  end
end
