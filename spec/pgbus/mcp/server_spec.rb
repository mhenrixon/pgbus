# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Server do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  describe ".build" do
    subject(:server) { described_class.build(data_source: data_source) }

    it "names the server pgbus and stamps the gem version" do
      expect(server.name).to eq("pgbus")
      expect(server.version).to eq(Pgbus::VERSION)
    end

    it "registers exactly the curated diagnostic tool set" do
      expect(server.tools.keys).to match_array(described_class::TOOLS.map(&:tool_name))
    end

    it "injects the data source and defaults payloads to disallowed" do
      expect(server.server_context[:data_source]).to eq(data_source)
      expect(server.server_context[:allow_payloads]).to be(false)
    end

    it "honors allow_payloads when requested" do
      server = described_class.build(data_source: data_source, allow_payloads: true)
      expect(server.server_context[:allow_payloads]).to be(true)
    end

    it "every registered tool is read-only and non-destructive" do
      described_class::TOOLS.each do |tool|
        annotations = tool.annotations_value.to_h
        expect(annotations[:readOnlyHint]).to be(true), "#{tool.tool_name} is not read-only"
        expect(annotations[:destructiveHint]).to be(false), "#{tool.tool_name} is destructive"
      end
    end

    it "exposes no tool that accepts raw SQL or mutates state" do
      names = described_class::TOOLS.map(&:tool_name)
      expect(names).to all(start_with("pgbus_"))
      expect(names).not_to include(a_string_matching(/sql|query|exec|purge|retry|discard|delete|drop|pause|resume/))
    end
  end

  describe "tool listing over the protocol" do
    subject(:server) { described_class.build(data_source: data_source) }

    it "lists all tools via a tools/list JSON-RPC request" do
      request = { jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }
      response = JSON.parse(server.handle_json(JSON.generate(request)))

      tool_names = response.dig("result", "tools").map { |t| t["name"] }
      expect(tool_names).to include("pgbus_health", "pgbus_queues", "pgbus_processes")
    end

    it "calls pgbus_health end-to-end through the server" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {}, stream_queue_names: Set.new)

      request = {
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "pgbus_health", arguments: {} }
      }
      response = JSON.parse(server.handle_json(JSON.generate(request)))

      text = response.dig("result", "content").first["text"]
      expect(JSON.parse(text)["status"]).to eq("OK")
    end
  end
end
