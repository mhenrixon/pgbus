# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Outbox::Poller do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:poller) { described_class.new }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      poller.graceful_shutdown
      expect(poller.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#poll_and_publish" do
    let(:outbox_entry_class) { stub_const("Pgbus::OutboxEntry", Class.new) }
    let(:relation) { double("relation") }

    before do
      outbox_entry_class
      allow(Pgbus::OutboxEntry).to receive(:unpublished).and_return(relation)
      allow(Pgbus::OutboxEntry).to receive(:transaction).and_yield
      allow(relation).to receive_messages(order: relation, limit: relation, lock: relation)
    end

    it "publishes entries to PGMQ via queue_name" do
      entry = double("entry",
                     id: 1,
                     queue_name: "default",
                     routing_key: nil,
                     payload: { "data" => "test" },
                     headers: nil,
                     delay: 0,
                     priority: 1,
                     update!: true)
      allow(relation).to receive(:to_a).and_return([entry], [])

      result = poller.poll_and_publish

      expect(mock_client).to have_received(:send_message).with("default", { "data" => "test" }, headers: nil, delay: 0, priority: 1)
      expect(entry).to have_received(:update!).with(published_at: a_kind_of(Time))
      expect(result).to eq(1)
    end

    it "publishes entries via routing_key" do
      routing_key = "orders.created"
      entry = double("entry",
                     id: 2,
                     queue_name: nil,
                     routing_key: routing_key,
                     payload: { "event" => "data" },
                     headers: { "trace" => "id" },
                     delay: nil,
                     priority: 0,
                     update!: true)
      allow(relation).to receive(:to_a).and_return([entry], [])

      result = poller.poll_and_publish

      expect(mock_client).to have_received(:publish_to_topic).with("orders.created", { "event" => "data" }, headers: { "trace" => "id" }, delay: 0)
      expect(result).to eq(1)
    end

    it "returns 0 when no entries" do
      allow(relation).to receive(:to_a).and_return([])

      result = poller.poll_and_publish
      expect(result).to eq(0)
    end

    it "rescues errors gracefully" do
      allow(relation).to receive(:to_a).and_raise(StandardError, "db error")

      expect { poller.poll_and_publish }.not_to raise_error
      expect(poller.poll_and_publish).to eq(0)
    end
  end
end
