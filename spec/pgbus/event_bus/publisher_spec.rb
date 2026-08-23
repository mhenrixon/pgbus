# frozen_string_literal: true

require "spec_helper"
require "active_job"

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
        hash_including("event_id" => a_kind_of(String), "payload" => { "order_id" => 1 },
                       "routing_key" => "orders.created"),
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

  describe ".publish with config.event_fair_share (issue #427)" do
    let(:tenant_payload) { { "order_id" => 1, "tenant_id" => 42 } }

    after do
      Pgbus.configuration.event_fair_share = nil
      Pgbus.configuration.fair_share = nil
    end

    def published_envelope
      envelope = nil
      expect(mock_client).to have_received(:publish_to_topic) { |_rk, data, **_opts| envelope = data }
      envelope
    end

    it "tags the envelope with the resolved key (and weight when not 1)" do
      Pgbus.configuration.event_fair_share = ->(e) { [e.payload["tenant_id"], 3] }

      described_class.publish("orders.created", tenant_payload)

      expect(published_envelope).to include("pgbus_fair_key" => "42", "pgbus_fair_weight" => 3)
      expect(published_envelope["payload"]).to eq(tenant_payload)
    end

    it "hands the callable a Pgbus::Event carrying routing_key, the original payload, and headers" do
      seen = nil
      Pgbus.configuration.event_fair_share = lambda { |e|
        seen = e
        "t"
      }

      described_class.publish("orders.created", tenant_payload, headers: { "x-trace" => "abc" })

      expect(seen).to be_a(Pgbus::Event)
      expect(seen.routing_key).to eq("orders.created")
      expect(seen.payload).to equal(tenant_payload)
      expect(seen.headers).to eq("x-trace" => "abc")
      expect(seen.event_id).to eq(published_envelope["event_id"])
    end

    it "gives the callable the original object for GlobalID payloads (not the serialized form)" do
      gid = double("GlobalID", to_s: "gid://app/Order/1")
      record = double("Order", to_global_id: gid, tenant_id: 9)
      allow(record).to receive(:respond_to?).with(:to_global_id).and_return(true)
      Pgbus.configuration.event_fair_share = ->(e) { e.payload.tenant_id }

      described_class.publish("orders.created", record)

      expect(published_envelope).to include("pgbus_fair_key" => "9", "payload" => { "_global_id" => "gid://app/Order/1" })
    end

    it "leaves the envelope untagged when the callable returns nil" do
      Pgbus.configuration.event_fair_share = ->(_e) {}

      described_class.publish("orders.created", tenant_payload)

      expect(published_envelope).not_to have_key("pgbus_fair_key")
    end

    it "leaves the envelope untagged when event_fair_share is nil even if fair_share (jobs) is set" do
      Pgbus.configuration.fair_share = ->(_job) { "job-tenant" }

      described_class.publish("orders.created", tenant_payload)

      expect(published_envelope).not_to have_key("pgbus_fair_key")
    end

    it "propagates an exception raised by the callable (nothing is published)" do
      Pgbus.configuration.event_fair_share = ->(_e) { raise "no tenant" }

      expect { described_class.publish("orders.created", tenant_payload) }.to raise_error(RuntimeError, "no tenant")
      expect(mock_client).not_to have_received(:publish_to_topic)
    end
  end

  describe ".publish with config.current_attributes (issue #431)" do
    let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id } }

    def published_envelope
      envelope = nil
      expect(mock_client).to have_received(:publish_to_topic) { |_rk, data, **_opts| envelope = data }
      envelope
    end

    before do
      stub_const("PubSpecCurrent", current_class)
      Pgbus.configuration.current_attributes = ["PubSpecCurrent"]
    end

    after do
      Pgbus.configuration.current_attributes = nil
      ActiveSupport::CurrentAttributes.clear_all
    end

    it "captures the publisher's Current into the envelope under pgbus_current, leaving the payload untouched" do
      PubSpecCurrent.tenant = "acme"
      PubSpecCurrent.request_id = "req-1"

      described_class.publish("orders.created", { "order_id" => 1 })

      env = published_envelope
      expect(env["pgbus_current"]).to include("PubSpecCurrent" => hash_including("tenant" => "acme", "request_id" => "req-1"))
      expect(env["payload"]).to eq("order_id" => 1)
    end

    it "adds nothing when no attribute is assigned" do
      described_class.publish("orders.created", { "order_id" => 1 })

      expect(published_envelope).not_to have_key("pgbus_current")
    end

    it "adds nothing when current_attributes is off" do
      Pgbus.configuration.current_attributes = nil
      PubSpecCurrent.tenant = "acme"

      described_class.publish("orders.created", { "order_id" => 1 })

      expect(published_envelope).not_to have_key("pgbus_current")
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

    it "includes routing_key when provided" do
      result = described_class.build_event_data({ "foo" => "bar" }, routing_key: "orders.created")

      expect(result["routing_key"]).to eq("orders.created")
    end

    it "omits routing_key when not provided" do
      result = described_class.build_event_data({ "foo" => "bar" })

      expect(result).not_to have_key("routing_key")
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

  describe "Pgbus.publish / Pgbus.publish_later (top-level shortcuts)" do
    it "Pgbus.publish delegates to EventBus::Publisher.publish" do
      allow(described_class).to receive(:publish).and_call_original

      Pgbus.publish("orders.created", { "order_id" => 1 })

      expect(described_class).to have_received(:publish).with(
        "orders.created", { "order_id" => 1 }, headers: nil, delay: 0
      )
    end

    it "Pgbus.publish forwards headers and delay" do
      allow(described_class).to receive(:publish).and_call_original

      Pgbus.publish("orders.created", { "order_id" => 1 }, headers: { "x" => "y" }, delay: 30)

      expect(described_class).to have_received(:publish).with(
        "orders.created", { "order_id" => 1 }, headers: { "x" => "y" }, delay: 30
      )
    end

    it "Pgbus.publish_later delegates to EventBus::Publisher.publish_later with a required delay" do
      allow(described_class).to receive(:publish_later).and_call_original

      Pgbus.publish_later("orders.shipped", { "order_id" => 2 }, delay: 60)

      expect(described_class).to have_received(:publish_later).with(
        "orders.shipped", { "order_id" => 2 }, delay: 60, headers: nil
      )
    end
  end
end
