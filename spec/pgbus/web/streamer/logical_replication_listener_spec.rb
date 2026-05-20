# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe Pgbus::Web::Streamer::LogicalReplicationListener do
  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }
  let(:fake_pg)        { Class.new { def close; end; def closed?; false; end }.new }

  subject(:listener) do
    described_class.new(
      pg_connection: fake_pg,
      dispatch_queue: dispatch_queue,
      health_check_ms: 50,
      logger: logger,
      slot_name: "pgbus_streamer_test_slot"
    )
  end

  describe "interest set (LISTEN equivalent)" do
    it "tracks ensure_listening / remove_listening" do
      listener.ensure_listening("chat")
      listener.ensure_listening("metrics")
      expect(listener.listening_to).to contain_exactly("chat", "metrics")

      listener.remove_listening("chat")
      expect(listener.listening_to).to contain_exactly("metrics")
    end
  end

  describe "pgoutput RELATION + INSERT parsing" do
    # Drive the private parser directly. We hand it a synthetic pgoutput
    # message that matches the wire format pgoutput emits for:
    #   - a RELATION row for `pgmq.q_pgbus_chat`
    #   - an INSERT into that relation
    let(:relation_oid) { 12_345 }

    let(:relation_message) do
      # 'R' Int32(oid) String(schema) String(table) Int8(replica_identity)
      #     Int16(natts) [Int8(flags) String(name) Int32(type) Int32(typmod)] x natts
      payload = String.new
      payload << "R"
      payload << [relation_oid].pack("N")
      payload << "pgmq\x00"
      payload << "q_pgbus_chat\x00"
      payload << [0].pack("C")        # replica_identity
      payload << [0].pack("n")        # natts = 0 (sufficient for the spike — we don't read columns)
      payload
    end

    let(:insert_message) do
      # 'I' Int32(relation_oid) Byte('N') TupleData
      # We don't read the tuple so an empty Int16 col count is enough.
      payload = String.new
      payload << "I"
      payload << [relation_oid].pack("N")
      payload << "N"
      payload << [0].pack("n") # 0 columns
      payload
    end

    it "registers a RELATION and emits WakeMessage on a matching INSERT for a listened queue" do
      listener.ensure_listening("chat")
      listener.send(:handle_pgoutput_message, relation_message)
      listener.send(:handle_pgoutput_message, insert_message)

      msg = dispatch_queue.pop(true)
      expect(msg).to be_a(Pgbus::Web::Streamer::LogicalReplicationListener::WakeMessage)
      expect(msg.queue_name).to eq("chat")
    end

    it "does NOT emit WakeMessage for a queue not in the interest set" do
      # No ensure_listening call.
      listener.send(:handle_pgoutput_message, relation_message)
      listener.send(:handle_pgoutput_message, insert_message)

      expect { dispatch_queue.pop(true) }.to raise_error(ThreadError) # empty queue
    end

    it "ignores non-pgmq tables" do
      other_oid = 99_999
      foreign_relation = String.new
      foreign_relation << "R"
      foreign_relation << [other_oid].pack("N")
      foreign_relation << "public\x00"
      foreign_relation << "some_other_table\x00"
      foreign_relation << [0].pack("C")
      foreign_relation << [0].pack("n")

      foreign_insert = String.new
      foreign_insert << "I"
      foreign_insert << [other_oid].pack("N")
      foreign_insert << "N"
      foreign_insert << [0].pack("n")

      listener.send(:handle_pgoutput_message, foreign_relation)
      listener.send(:handle_pgoutput_message, foreign_insert)

      expect { dispatch_queue.pop(true) }.to raise_error(ThreadError)
    end

    it "ignores INSERTs for relations it has never seen a RELATION message for" do
      listener.ensure_listening("chat")
      listener.send(:handle_pgoutput_message, insert_message)
      # No RELATION processed first → no oid → queue_name mapping → drop.
      expect { dispatch_queue.pop(true) }.to raise_error(ThreadError)
    end
  end

  describe "WakeMessage compatibility" do
    it "uses the same struct as the LISTEN-based Listener so the dispatcher sees no difference" do
      expect(described_class::WakeMessage).to equal(Pgbus::Web::Streamer::Listener::WakeMessage)
    end
  end
end
