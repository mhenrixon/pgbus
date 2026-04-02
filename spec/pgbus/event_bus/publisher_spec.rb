# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::EventBus::Publisher do
  include PgmqDoubles

  let(:mock_pgmq) { build_mock_pgmq }
  let(:mock_client) { build_mock_client(pgmq: mock_pgmq) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe ".publish" do
    it "publishes via publish_to_topic and never falls back to send_message" do
      described_class.publish("orders.created", { "order_id" => 1 })

      expect(mock_client).to have_received(:publish_to_topic).with(
        "orders.created",
        hash_including("event_id" => a_kind_of(String), "payload" => { "order_id" => 1 }),
        headers: nil,
        delay: 0
      )
      expect(mock_client).not_to have_received(:send_message)
    end

    it "forwards headers" do
      described_class.publish("orders.created", { "order_id" => 1 }, headers: { "x-trace" => "abc" })

      expect(mock_client).to have_received(:publish_to_topic).with(
        "orders.created",
        hash_including("payload" => { "order_id" => 1 }),
        headers: { "x-trace" => "abc" },
        delay: 0
      )
    end

    it "forwards delay" do
      described_class.publish("orders.created", { "order_id" => 1 }, delay: 30)

      expect(mock_client).to have_received(:publish_to_topic).with(
        "orders.created",
        hash_including("payload" => { "order_id" => 1 }),
        headers: nil,
        delay: 30
      )
    end
  end

  describe ".publish_later" do
    it "delegates to .publish with the specified delay" do
      described_class.publish_later("orders.shipped", { "order_id" => 2 }, delay: 60)

      expect(mock_client).to have_received(:publish_to_topic).with(
        "orders.shipped",
        hash_including("payload" => { "order_id" => 2 }),
        headers: nil,
        delay: 60
      )
    end

    it "forwards headers" do
      described_class.publish_later("orders.shipped", { "id" => 3 }, delay: 10, headers: { "x-src" => "test" })

      expect(mock_client).to have_received(:publish_to_topic).with(
        "orders.shipped",
        hash_including("payload" => { "id" => 3 }),
        headers: { "x-src" => "test" },
        delay: 10
      )
    end
  end

  describe ".build_event_data" do
    it "wraps Hash payloads directly" do
      result = described_class.build_event_data({ "foo" => "bar" })

      expect(result).to include("event_id" => a_kind_of(String), "published_at" => a_kind_of(String))
      expect(result["payload"]).to eq("foo" => "bar")
    end

    it "wraps GlobalID-capable payloads with _global_id key" do
      gid = double("GlobalID", to_s: "gid://app/User/42")
      payload_obj = double("User", to_global_id: gid)
      allow(payload_obj).to receive(:respond_to?).with(:to_global_id).and_return(true)

      result = described_class.build_event_data(payload_obj)

      expect(result["payload"]).to eq("_global_id" => "gid://app/User/42")
    end

    it "wraps plain values in a value key" do
      result = described_class.build_event_data("simple_string")

      expect(result["payload"]).to eq("value" => "simple_string")
    end

    it "wraps numeric plain values in a value key" do
      result = described_class.build_event_data(42)

      expect(result["payload"]).to eq("value" => 42)
    end

    it "generates a unique event_id each time" do
      result1 = described_class.build_event_data("a")
      result2 = described_class.build_event_data("b")

      expect(result1["event_id"]).not_to eq(result2["event_id"])
    end

    it "includes a published_at timestamp in ISO 8601 format" do
      result = described_class.build_event_data("test")

      expect(result["published_at"]).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)
    end
  end
end
