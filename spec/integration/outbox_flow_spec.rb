# frozen_string_literal: true

require_relative "../integration_helper"

# End-to-end coverage of the transactional outbox: an OutboxEntry written
# inside an AR transaction is picked up by the Poller and published to PGMQ,
# where it becomes readable through Pgbus::Client. Previously the poller was
# only exercised with a stubbed client (spec/pgbus/outbox/poller_spec.rb).
RSpec.describe "Outbox flow (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:poller) { Pgbus::Outbox::Poller.new }

  before do
    client.ensure_queue("outbox_target")
  end

  describe "direct-queue publishing" do
    it "publishes a queued entry to PGMQ and marks it published" do
      entry = nil
      Pgbus::OutboxEntry.transaction do
        entry = Pgbus::Outbox.publish("outbox_target", { "order_id" => 42 })
      end

      expect(entry.published_at).to be_nil

      published = poller.poll_and_publish
      expect(published).to eq(1)

      message = client.read_message("outbox_target")
      expect(message).not_to be_nil
      expect(JSON.parse(message.message)).to include("order_id" => 42)

      expect(entry.reload.published_at).not_to be_nil
    end

    it "batches multiple entries for the same queue into one send" do
      Pgbus::OutboxEntry.transaction do
        Pgbus::Outbox.publish("outbox_target", { "n" => 1 })
        Pgbus::Outbox.publish("outbox_target", { "n" => 2 })
        Pgbus::Outbox.publish("outbox_target", { "n" => 3 })
      end

      expect(poller.poll_and_publish).to eq(3)

      messages = client.read_batch("outbox_target", qty: 10)
      values = messages.map { |m| JSON.parse(m.message)["n"] }
      expect(values).to contain_exactly(1, 2, 3)

      expect(Pgbus::OutboxEntry.unpublished.count).to eq(0)
    end
  end

  describe "topic-routed publishing" do
    let(:consumer_queue) { "outbox_events" }

    before do
      client.ensure_queue(consumer_queue)
      client.bind_topic("orders.created", consumer_queue)
    end

    it "publishes an event entry to the bound topic queue" do
      Pgbus::OutboxEntry.transaction do
        Pgbus::Outbox.publish_event("orders.created", { "order_id" => 7 })
      end

      expect(poller.poll_and_publish).to eq(1)

      message = client.read_message(consumer_queue)
      expect(message).not_to be_nil

      parsed = JSON.parse(message.message)
      expect(parsed.dig("payload", "order_id")).to eq(7)
      expect(parsed["event_id"]).to be_present
    end
  end

  describe "failure path" do
    it "leaves the entry unpublished when publishing raises" do
      Pgbus::OutboxEntry.transaction do
        Pgbus::Outbox.publish("outbox_target", { "will" => "retry" })
      end

      allow(client).to receive(:send_batch).and_raise(StandardError, "pgmq down")
      allow(client).to receive(:send_message).and_raise(StandardError, "pgmq down")
      allow(Pgbus).to receive(:client).and_return(client)

      expect(poller.poll_and_publish).to eq(0)
      expect(Pgbus::OutboxEntry.unpublished.count).to eq(1)
    end
  end
end
