# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Thread safety (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("thread_test")
  end

  describe "concurrent enqueue" do
    it "handles concurrent sends without corruption" do
      errors = Concurrent::Array.new

      threads = 10.times.map do |i|
        Thread.new do
          client.send_message("thread_test", { "thread" => i, "job_class" => "ThreadJob" })
        rescue StandardError => e
          errors << e
        end
      end

      threads.each { |t| t.join(10) }

      expect(errors).to be_empty

      # All 10 messages should be in the queue
      all_messages = client.read_batch("thread_test", qty: 20, vt: 0) || []
      expect(all_messages.size).to eq(10)
    end
  end

  describe "concurrent read + archive" do
    it "each message is read by exactly one thread", timeout: 15 do
      # Enqueue 10 messages
      10.times { |i| client.send_message("thread_test", { "index" => i }) }

      claimed = Concurrent::Array.new

      threads = 5.times.map do
        Thread.new do
          3.times do
            msgs = client.read_batch("thread_test", qty: 1, vt: 30)
            next if msgs.nil? || msgs.empty?

            msg = msgs.first
            claimed << msg.msg_id.to_i
            client.archive_message("thread_test", msg.msg_id.to_i)
          end
        end
      end

      threads.each { |t| t.join(10) }

      # No duplicates — each message claimed by exactly one thread
      expect(claimed.size).to eq(claimed.uniq.size)
    end
  end

  describe "mutex serialization" do
    it "does not produce PG errors under concurrent operations", timeout: 15 do
      errors = Concurrent::Array.new

      threads = 3.times.map do |i|
        Thread.new do
          5.times do |j|
            client.send_message("thread_test", { "t" => i, "j" => j })
            client.read_batch("thread_test", qty: 1, vt: 0)
          end
        rescue StandardError => e
          errors << "#{e.class}: #{e.message}"
        end
      end

      threads.each { |t| t.join(10) }

      pg_errors = errors.select { |e| e.include?("PG::") || e.include?("Segfault") }
      expect(pg_errors).to be_empty
    end
  end
end
