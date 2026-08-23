# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Outbox do
  let(:outbox_entry_class) { stub_const("Pgbus::OutboxEntry", Class.new) }
  let(:mock_client) { build_mock_client }

  before do
    outbox_entry_class
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::OutboxEntry).to receive(:create!).and_return(double("entry", id: 1))
  end

  describe ".publish" do
    it "creates an outbox entry with queue_name" do
      described_class.publish("default", { "key" => "value" })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        queue_name: "default",
        payload: { "key" => "value" },
        headers: nil,
        priority: 1,
        delay: 0
      )
    end

    it "passes priority and delay" do
      described_class.publish("default", { "data" => 1 }, priority: 0, delay: 30)

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        queue_name: "default",
        payload: { "data" => 1 },
        headers: nil,
        priority: 0,
        delay: 30
      )
    end

    it "passes headers" do
      described_class.publish("default", "body", headers: { "trace" => "abc" })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        queue_name: "default",
        payload: "body",
        headers: { "trace" => "abc" },
        priority: 1,
        delay: 0
      )
    end
  end

  describe ".publish_event" do
    before do
      allow(Pgbus::EventBus::Publisher).to receive(:build_event_data).and_return(
        { "event_id" => "uuid", "payload" => { "data" => 1 }, "published_at" => "2026-01-01" }
      )
    end

    after { Pgbus.configuration.event_fair_share = nil }

    it "builds the envelope WITH the routing key so the consumer can dispatch the relayed event" do
      allow(Pgbus::EventBus::Publisher).to receive(:build_event_data).and_call_original

      described_class.publish_event("orders.created", { "id" => 42 })

      expect(Pgbus::EventBus::Publisher).to have_received(:build_event_data)
        .with({ "id" => 42 }, routing_key: "orders.created")
    end

    it "creates an outbox entry with routing_key" do
      described_class.publish_event("orders.created", { "id" => 42 })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        routing_key: "orders.created",
        payload: { "event_id" => "uuid", "payload" => { "data" => 1 }, "published_at" => "2026-01-01" },
        headers: nil
      )
    end

    it "tags the stored envelope when config.event_fair_share is set (issue #427)" do
      Pgbus.configuration.event_fair_share = ->(e) { [e.payload["tenant_id"], 2] }

      described_class.publish_event("orders.created", { "tenant_id" => 7 })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        routing_key: "orders.created",
        payload: { "event_id" => "uuid", "payload" => { "data" => 1 }, "published_at" => "2026-01-01",
                   "pgbus_fair_key" => "7", "pgbus_fair_weight" => 2 },
        headers: nil
      )
    end

    it "hands the callable an Event with the routing_key (it is not in the stored envelope)" do
      seen = nil
      Pgbus.configuration.event_fair_share = lambda { |e|
        seen = e
        "t"
      }

      described_class.publish_event("orders.created", { "tenant_id" => 7 }, headers: { "h" => 1 })

      expect(seen.routing_key).to eq("orders.created")
      expect(seen.payload).to eq("tenant_id" => 7)
      expect(seen.headers).to eq("h" => 1)
      expect(seen.event_id).to eq("uuid")
    end

    it "captures Current into the stored envelope when config.current_attributes is set (issue #431)" do
      stub_const("OutboxSpecCurrent", Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant })
      Pgbus.configuration.current_attributes = ["OutboxSpecCurrent"]
      OutboxSpecCurrent.tenant = "acme"

      described_class.publish_event("orders.created", { "tenant_id" => 7 })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        routing_key: "orders.created",
        payload: hash_including("event_id" => "uuid",
                                "pgbus_current" => { "OutboxSpecCurrent" => hash_including("tenant" => "acme") }),
        headers: nil
      )
    ensure
      Pgbus.configuration.current_attributes = nil
      ActiveSupport::CurrentAttributes.clear_all
    end
  end
end
