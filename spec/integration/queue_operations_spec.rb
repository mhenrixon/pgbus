# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Queue operations (integration)", :integration do
  let(:client) { Pgbus.client }

  describe "queue creation" do
    it "creates a queue idempotently" do
      client.ensure_queue("ops_test")
      client.ensure_queue("ops_test") # second call should not raise
      expect(client.list_queues.map(&:to_s)).to include("pgbus_int_ops_test")
    end

    it "creates a dead letter queue" do
      client.ensure_dead_letter_queue("ops_test")
      expect(client.list_queues.map(&:to_s)).to include("pgbus_int_ops_test_dlq")
    end
  end

  describe "batch operations" do
    before { client.ensure_queue("batch_test") }

    it "sends and reads a batch of messages" do
      payloads = 5.times.map { |i| { "index" => i, "job_class" => "BatchJob" } }
      msg_ids = client.send_batch("batch_test", payloads)

      expect(msg_ids.size).to eq(5)

      messages = client.read_batch("batch_test", qty: 10)
      expect(messages.size).to eq(5)

      indices = messages.map { |m| JSON.parse(m.message)["index"] }
      expect(indices.sort).to eq([0, 1, 2, 3, 4])
    end

    it "respects qty limit on read" do
      5.times { |i| client.send_message("batch_test", { "index" => i }) }

      messages = client.read_batch("batch_test", qty: 2)
      expect(messages.size).to eq(2)
    end
  end

  describe "purge" do
    before { client.ensure_queue("purge_test") }

    it "removes all messages from a queue" do
      3.times { client.send_message("purge_test", { "data" => "x" }) }

      client.purge_queue("purge_test")

      messages = client.read_batch("purge_test", qty: 10)
      expect(messages || []).to be_empty
    end
  end

  describe "visibility timeout" do
    before { client.ensure_queue("vt_test") }

    it "makes message invisible for the specified duration" do
      client.send_message("vt_test", { "vt" => true })

      # Read with VT=30s
      messages = client.read_batch("vt_test", qty: 1, vt: 30)
      expect(messages.size).to eq(1)

      # Immediately re-read — should be empty (VT hasn't expired)
      again = client.read_batch("vt_test", qty: 1, vt: 30)
      expect(again || []).to be_empty
    end

    it "allows extending visibility timeout" do
      msg_id = client.send_message("vt_test", { "extend" => true })
      client.read_batch("vt_test", qty: 1, vt: 5)

      # Extend VT to 60s
      client.extend_visibility("vt_test", msg_id, vt: 60)

      # Should still be invisible
      messages = client.read_batch("vt_test", qty: 1, vt: 5)
      expect(messages || []).to be_empty
    end
  end

  describe "delayed messages" do
    before { client.ensure_queue("delay_test") }

    it "sends a message with a delay" do
      client.send_message("delay_test", { "delayed" => true }, delay: 60)

      # Should not be visible yet (60s delay)
      messages = client.read_batch("delay_test", qty: 1)
      expect(messages || []).to be_empty
    end
  end

  describe "message headers" do
    before { client.ensure_queue("header_test") }

    it "preserves headers through enqueue and read" do
      headers = { "x-trace-id" => "abc-123", "x-source" => "test" }
      client.send_message("header_test", { "data" => 1 }, headers: headers)

      messages = client.read_batch("header_test", qty: 1)
      expect(messages.size).to eq(1)

      msg_headers = messages.first.headers
      parsed = msg_headers.is_a?(String) ? JSON.parse(msg_headers) : msg_headers
      expect(parsed).to include("x-trace-id" => "abc-123")
    end
  end
end
