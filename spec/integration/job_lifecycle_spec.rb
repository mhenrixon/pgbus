# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job lifecycle (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("default")
  end

  describe "enqueue and read" do
    it "sends a message to PGMQ and reads it back" do
      payload = { "job_class" => "TestJob", "job_id" => SecureRandom.uuid, "arguments" => [1, 2] }
      msg_id = client.send_message("default", payload)
      expect(msg_id.to_i).to be_positive

      messages = client.read_batch("default", qty: 1)
      expect(messages).not_to be_empty
      expect(messages.first.msg_id.to_i).to eq(msg_id.to_i)

      parsed = JSON.parse(messages.first.message)
      expect(parsed["job_class"]).to eq("TestJob")
    end

    it "respects visibility timeout — message is invisible after read" do
      client.send_message("default", { "test" => true })
      messages = client.read_batch("default", qty: 1, vt: 30)
      expect(messages.size).to eq(1)

      # Message was read with VT=30s — reading again immediately should return empty
      # because the message is invisible until VT expires
      second_read = client.read_batch("default", qty: 1, vt: 30)
      expect(second_read || []).to be_empty
    end
  end

  describe "archive and delete" do
    it "archives a message after processing" do
      msg_id = client.send_message("default", { "test" => "archive" })
      messages = client.read_batch("default", qty: 1)
      expect(messages.size).to eq(1)

      client.archive_message("default", msg_id)

      # Queue should be empty now
      remaining = client.read_batch("default", qty: 1)
      expect(remaining).to be_empty
    end
  end

  describe "dead letter queue" do
    it "moves a message to DLQ" do
      client.send_message("default", { "dlq_test" => true })
      messages = client.read_batch("default", qty: 1)

      client.move_to_dead_letter("default", messages.first)

      # Original queue should be empty
      remaining = client.read_batch("default", qty: 1)
      expect(remaining).to be_empty
    end
  end
end
