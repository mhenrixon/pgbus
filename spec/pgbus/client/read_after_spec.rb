# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::ReadAfter do
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
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.streams_queue_prefix = "pgbus_stream"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn)  { double("raw_conn") }

  describe "#read_after" do
    let(:live_row) do
      {
        "msg_id" => "1248",
        "enqueued_at" => "2026-04-07T10:00:00.000000Z",
        "message" => '{"html":"<turbo-stream>A</turbo-stream>"}',
        "source" => "live"
      }
    end
    let(:archive_row) do
      {
        "msg_id" => "1249",
        "enqueued_at" => "2026-04-07T10:00:01.000000Z",
        "message" => '{"html":"<turbo-stream>B</turbo-stream>"}',
        "source" => "live"
      }
    end

    it "queries q_ and a_ tables with msg_id > after_id and returns Envelope objects" do
      allow(raw_conn).to receive(:exec_params)
        .and_return(double("PG::Result", to_a: [live_row, archive_row]))

      envelopes = client.read_after("chat", after_id: 1247)

      expect(raw_conn).to have_received(:exec_params)
        .with(a_string_matching(/pgmq\.q_pgbus_test_chat.*pgmq\.a_pgbus_test_chat/m), [1247, 500])
      expect(envelopes.length).to eq(2)
      expect(envelopes.first).to be_a(Pgbus::Client::ReadAfter::Envelope)
      expect(envelopes.first.msg_id).to eq(1248)
      expect(envelopes.first.payload).to eq(live_row["message"])
      expect(envelopes.first.source).to eq("live")
      expect(envelopes.last.msg_id).to eq(1249)
    end

    it "respects a custom limit" do
      allow(raw_conn).to receive(:exec_params)
        .and_return(double("PG::Result", to_a: []))

      client.read_after("chat", after_id: 0, limit: 50)

      expect(raw_conn).to have_received(:exec_params).with(anything, [0, 50])
    end

    it "returns an empty array when no rows match" do
      allow(raw_conn).to receive(:exec_params).and_return(double("PG::Result", to_a: []))
      expect(client.read_after("chat", after_id: 9999)).to eq([])
    end

    it "rejects unsanitised stream names via QueueNameValidator" do
      expect { client.read_after("bad'; DROP TABLE", after_id: 0) }
        .to raise_error(ArgumentError)
    end

    it "passes after_id and limit as positional bind parameters" do
      # This test exists to lock in that we're using parameterised queries — not
      # string interpolation — for the WHERE msg_id > $1 clause. SQL injection
      # protection is critical because stream names come from URLs.
      received_params = nil
      allow(raw_conn).to receive(:exec_params) do |_sql, params|
        received_params = params
        double("PG::Result", to_a: [])
      end

      client.read_after("chat", after_id: 42, limit: 7)

      expect(received_params).to eq([42, 7])
    end

    it "produces SQL that orders the unioned rows by msg_id ASC" do
      received_sql = nil
      allow(raw_conn).to receive(:exec_params) do |sql, _|
        received_sql = sql
        double("PG::Result", to_a: [])
      end

      client.read_after("chat", after_id: 0)

      expect(received_sql).to match(/ORDER BY msg_id\s+ASC/i)
    end

    it "preserves source attribution from the union" do
      live = live_row.merge("source" => "live")
      archived = archive_row.merge("source" => "archive")
      allow(raw_conn).to receive(:exec_params)
        .and_return(double("PG::Result", to_a: [live, archived]))

      envelopes = client.read_after("chat", after_id: 1247)
      expect(envelopes.map(&:source)).to eq(%w[live archive])
    end
  end

  describe "#stream_current_msg_id" do
    it "returns COALESCE(MAX(msg_id), 0) from the live queue table" do
      result = double("PG::Result")
      allow(result).to receive(:first).and_return({ "max" => "1247" })
      allow(raw_conn).to receive(:exec).and_return(result)

      value = client.stream_current_msg_id("chat")

      expect(value).to eq(1247)
      expect(raw_conn).to have_received(:exec)
        .with(a_string_matching(/COALESCE\(MAX\(msg_id\), 0\).*pgmq\.q_pgbus_test_chat/m))
    end

    it "returns 0 when the queue is empty" do
      result = double("PG::Result")
      allow(result).to receive(:first).and_return({ "max" => "0" })
      allow(raw_conn).to receive(:exec).and_return(result)
      expect(client.stream_current_msg_id("chat")).to eq(0)
    end

    it "rejects unsafe queue names" do
      expect { client.stream_current_msg_id("nope; DROP") }.to raise_error(ArgumentError)
    end
  end

  describe "#stream_oldest_msg_id" do
    it "returns the LEAST of MIN(q_) and MIN(a_), or nil when both empty" do
      result = double("PG::Result")
      allow(result).to receive(:first).and_return({ "least" => "100" })
      allow(raw_conn).to receive(:exec).and_return(result)

      value = client.stream_oldest_msg_id("chat")

      expect(value).to eq(100)
      expect(raw_conn).to have_received(:exec)
        .with(a_string_matching(/LEAST.*MIN.*pgmq\.q_pgbus_test_chat.*MIN.*pgmq\.a_pgbus_test_chat/m))
    end

    it "returns nil when both tables are empty" do
      result = double("PG::Result")
      allow(result).to receive(:first).and_return({ "least" => nil })
      allow(raw_conn).to receive(:exec).and_return(result)
      expect(client.stream_oldest_msg_id("chat")).to be_nil
    end
  end
end
