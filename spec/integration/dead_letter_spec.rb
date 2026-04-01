# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Dead letter queue (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("dlq_test")
    client.ensure_dead_letter_queue("dlq_test")
  end

  describe "move to DLQ" do
    it "moves a message from the main queue to the DLQ" do
      client.send_message("dlq_test", { "job_class" => "FailingJob", "arguments" => [] })
      messages = client.read_batch("dlq_test", qty: 1)
      expect(messages.size).to eq(1)

      client.move_to_dead_letter("dlq_test", messages.first)

      # Main queue empty
      remaining = client.read_batch("dlq_test", qty: 1)
      expect(remaining || []).to be_empty

      # DLQ has the message
      dlq_name = Pgbus.configuration.dead_letter_queue_name("dlq_test")
      dlq_messages = client.read_batch(
        dlq_name.delete_prefix("#{Pgbus.configuration.queue_prefix}_"),
        qty: 1
      )
      expect(dlq_messages.size).to eq(1)

      payload = JSON.parse(dlq_messages.first.message)
      expect(payload["job_class"]).to eq("FailingJob")
    end
  end

  describe "message read_ct tracking" do
    it "increments read_ct on each read" do
      client.send_message("dlq_test", { "retry_test" => true })

      # First read
      msg1 = client.read_batch("dlq_test", qty: 1, vt: 0).first
      expect(msg1.read_ct.to_i).to eq(1)

      # Second read (VT=0 makes it immediately visible)
      msg2 = client.read_batch("dlq_test", qty: 1, vt: 0).first
      expect(msg2.read_ct.to_i).to eq(2)

      # Third read
      msg3 = client.read_batch("dlq_test", qty: 1, vt: 0).first
      expect(msg3.read_ct.to_i).to eq(3)
    end
  end
end
