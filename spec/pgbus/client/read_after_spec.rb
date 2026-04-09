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
    stub_const("PG::UndefinedTable", Class.new(StandardError)) unless defined?(PG::UndefinedTable)
    stub_const("PG::ConnectionBad", Class.new(StandardError)) unless defined?(PG::ConnectionBad)
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
        "source" => "archive"
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

    # Full replay from a fresh database: before any broadcast has happened,
    # pgmq.q_<stream> and pgmq.a_<stream> don't exist. A just-connected SSE
    # client asking for history must get an empty array, not a crash.
    # See issue #101.
    it "returns [] when the PGMQ queue tables do not exist yet" do
      allow(raw_conn).to receive(:exec_params)
        .and_raise(PG::UndefinedTable.new('relation "pgmq.q_pgbus_test_chat" does not exist'))

      expect(client.read_after("chat", after_id: 0)).to eq([])
    end

    it "re-raises PG::UndefinedTable for unrelated tables" do
      allow(raw_conn).to receive(:exec_params)
        .and_raise(PG::UndefinedTable.new('relation "public.unrelated" does not exist'))

      expect { client.read_after("chat", after_id: 0) }.to raise_error(PG::UndefinedTable)
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

    # A fresh database has not yet created pgmq.q_<stream> — the queue is
    # created lazily on the first broadcast. The canonical use case for
    # streams is `pgbus_stream_from Current.user` in a layout, which calls
    # current_msg_id on every page render to compute the watermark. On a
    # fresh database this query runs before any broadcast, so the queue
    # table does not yet exist. "No queue" is semantically equivalent to
    # "no messages", so the watermark must be 0 rather than an exception.
    # See issue #101.
    it "returns 0 when the PGMQ queue table does not exist yet" do
      allow(raw_conn).to receive(:exec)
        .and_raise(PG::UndefinedTable.new('relation "pgmq.q_pgbus_test_chat" does not exist'))

      expect(client.stream_current_msg_id("chat")).to eq(0)
    end

    it "returns 0 when ActiveRecord::StatementInvalid wraps a PG::UndefinedTable" do
      stub_const("ActiveRecord::StatementInvalid", Class.new(StandardError)) unless defined?(ActiveRecord::StatementInvalid)
      cause = PG::UndefinedTable.new('relation "pgmq.q_pgbus_test_chat" does not exist')
      wrapped = ActiveRecord::StatementInvalid.new(
        'PG::UndefinedTable: ERROR:  relation "pgmq.q_pgbus_test_chat" does not exist'
      )
      allow(wrapped).to receive(:cause).and_return(cause)
      allow(raw_conn).to receive(:exec).and_raise(wrapped)

      expect(client.stream_current_msg_id("chat")).to eq(0)
    end

    it "re-raises PG::UndefinedTable for unrelated tables" do
      allow(raw_conn).to receive(:exec)
        .and_raise(PG::UndefinedTable.new('relation "public.some_other_table" does not exist'))

      expect { client.stream_current_msg_id("chat") }.to raise_error(PG::UndefinedTable)
    end

    it "re-raises other PG errors" do
      allow(raw_conn).to receive(:exec).and_raise(PG::ConnectionBad.new("server closed"))

      expect { client.stream_current_msg_id("chat") }.to raise_error(PG::ConnectionBad)
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

    # Same missing-queue scenario as stream_current_msg_id — gap detection
    # is called on every replay request, and on a fresh database the queue
    # hasn't been created yet. See issue #101.
    it "returns nil when the PGMQ queue tables do not exist yet" do
      allow(raw_conn).to receive(:exec)
        .and_raise(PG::UndefinedTable.new('relation "pgmq.q_pgbus_test_chat" does not exist'))

      expect(client.stream_oldest_msg_id("chat")).to be_nil
    end

    it "re-raises PG::UndefinedTable for unrelated tables" do
      allow(raw_conn).to receive(:exec)
        .and_raise(PG::UndefinedTable.new('relation "public.unrelated" does not exist'))

      expect { client.stream_oldest_msg_id("chat") }.to raise_error(PG::UndefinedTable)
    end
  end
end
