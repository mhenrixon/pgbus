# frozen_string_literal: true

require "spec_helper"

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

    it "creates an outbox entry with routing_key" do
      described_class.publish_event("orders.created", { "id" => 42 })

      expect(Pgbus::OutboxEntry).to have_received(:create!).with(
        routing_key: "orders.created",
        payload: { "event_id" => "uuid", "payload" => { "data" => 1 }, "published_at" => "2026-01-01" },
        headers: nil
      )
    end
  end
end
