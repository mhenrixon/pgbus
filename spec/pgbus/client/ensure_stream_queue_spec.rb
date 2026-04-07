# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::EnsureStreamQueue do
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = Pgbus::Client.new(config)
    c.instance_variable_set(:@schema_ensured, true)
    c
  end

  before do
    allow_any_instance_of(Pgbus::Client).to receive(:require).with("pgmq").and_return(true)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
    allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
    allow(client).to receive(:ensure_queue)
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn)  { double("raw_conn", exec: nil) }

  describe "#ensure_stream_queue" do
    it "delegates queue creation to ensure_queue" do
      client.ensure_stream_queue("chat")
      expect(client).to have_received(:ensure_queue).with("chat")
    end

    it "creates the msg_id index on the archive table" do
      client.ensure_stream_queue("chat")

      expect(raw_conn).to have_received(:exec)
        .with(a_string_matching(/CREATE INDEX IF NOT EXISTS\s+a_pgbus_test_chat_msg_id_idx\s+ON pgmq\.a_pgbus_test_chat\s*\(msg_id\)/m))
    end

    it "is idempotent — calling twice runs ensure_queue twice (it has its own dedup) and CREATE INDEX IF NOT EXISTS twice" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("chat")

      expect(raw_conn).to have_received(:exec).twice
      expect(client).to have_received(:ensure_queue).twice
    end

    it "rejects unsanitised stream names via QueueNameValidator" do
      expect { client.ensure_stream_queue("nope; DROP TABLE") }
        .to raise_error(ArgumentError)
    end
  end
end
