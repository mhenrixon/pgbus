# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Thread safety (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("thread_test")
  end

  describe "concurrent enqueue" do
    it "handles concurrent sends without corruption" do
      barrier = Concurrent::CyclicBarrier.new(10)
      errors = Concurrent::Array.new

      threads = 10.times.map do |i|
        Thread.new do
          barrier.wait
          client.send_message("thread_test", { "thread" => i, "job_class" => "ThreadJob" })
        rescue StandardError => e
          errors << e
        end
      end

      threads.each(&:join)

      expect(errors).to be_empty

      # All 10 messages should be in the queue
      all_messages = []
      loop do
        batch = client.read_batch("thread_test", qty: 10, vt: 0)
        break if batch.nil? || batch.empty?

        all_messages.concat(batch)
        break if batch.size < 10
      end

      expect(all_messages.size).to eq(10)
    end
  end

  describe "concurrent read + archive" do
    it "each message is read by exactly one thread" do
      # Enqueue 20 messages
      20.times { |i| client.send_message("thread_test", { "index" => i }) }

      claimed = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(5)

      threads = 5.times.map do
        Thread.new do
          barrier.wait
          4.times do
            msgs = client.read_batch("thread_test", qty: 1, vt: 30)
            next if msgs.nil? || msgs.empty?

            msg = msgs.first
            claimed << msg.msg_id.to_i
            client.archive_message("thread_test", msg.msg_id.to_i)
          end
        end
      end

      threads.each(&:join)

      # No duplicates — each message claimed by exactly one thread
      expect(claimed.size).to eq(claimed.uniq.size)
    end
  end

  describe "mutex serialization" do
    it "does not segfault under concurrent PGMQ operations" do
      barrier = Concurrent::CyclicBarrier.new(5)
      errors = Concurrent::Array.new

      threads = 5.times.map do |i|
        Thread.new do
          barrier.wait
          10.times do |j|
            client.send_message("thread_test", { "t" => i, "j" => j })
            client.read_batch("thread_test", qty: 1, vt: 0)
          end
        rescue StandardError => e
          errors << "#{e.class}: #{e.message}"
        end
      end

      threads.each(&:join)

      # No segfaults or PG::Connection errors
      pg_errors = errors.select { |e| e.include?("PG::") || e.include?("Segfault") }
      expect(pg_errors).to be_empty
    end
  end
end
