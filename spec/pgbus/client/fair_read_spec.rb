# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::FairRead do
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = Pgbus::Client.new(config, schema_ensured: true)
    allow(c).to receive(:tune_autovacuum)
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    c
  end

  let(:fake_message_class) do
    Class.new do
      attr_reader :row

      def initialize(row)
        @row = row
      end

      def msg_id = row["msg_id"]
    end
  end
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.visibility_timeout = 45
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn) { double("raw_conn") }
  let(:rows) { [{ "msg_id" => "7", "message" => "{}" }, { "msg_id" => "9", "message" => "{}" }] }

  before do
    allow(Pgbus::Client).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
    stub_const("PGMQ::Message", fake_message_class)
    stub_const("PG::UndefinedTable", Class.new(StandardError)) unless defined?(PG::UndefinedTable)
    stub_const("PG::DuplicateTable", Class.new(StandardError)) unless defined?(PG::DuplicateTable)
    allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
  end

  # pgmq-ruby wraps PG errors in ConnectionError inside a rescue, so the PG
  # class rides along as #cause — the client detects both races by class.
  def wrapped_pg_error(pg_class, message)
    raise pg_class, message
  rescue pg_class
    raise PGMQ::Errors::ConnectionError, message
  end

  describe "#read_batch_fair" do
    before { allow(raw_conn).to receive(:exec_params).and_return(double("PG::Result", to_a: rows)) }

    it "runs the fair read against the prefixed queue table with [qty, vt] params" do
      client.read_batch_fair("default", qty: 5)

      expect(raw_conn).to have_received(:exec_params).with(a_string_including("pgmq.q_pgbus_test_default"), [5, 45])
    end

    it "honours an explicit vt" do
      client.read_batch_fair("default", qty: 2, vt: 10)

      expect(raw_conn).to have_received(:exec_params).with(anything, [2, 10])
    end

    it "maps rows through PGMQ::Message" do
      result = client.read_batch_fair("default", qty: 5)

      expect(result.map(&:class).uniq).to eq([fake_message_class])
      expect(result.map(&:msg_id)).to eq(%w[7 9])
    end

    it "returns an empty array when nothing is visible" do
      allow(raw_conn).to receive(:exec_params).and_return(double("PG::Result", to_a: []))
      expect(client.read_batch_fair("default", qty: 5)).to eq([])
    end

    it "orders candidates by rank / weight and locks with SKIP LOCKED" do
      client.read_batch_fair("default", qty: 5)

      expect(raw_conn).to have_received(:exec_params) do |sql, _params|
        expect(sql).to include("pgbus_fair_key")
        expect(sql).to include("pgbus_fair_weight")
        expect(sql).to include("FOR UPDATE OF m SKIP LOCKED")
        expect(sql).to include("ORDER BY rn / w, msg_id")
      end
    end

    it "instruments pgbus.client.read_batch_fair" do
      allow(Pgbus::Instrumentation).to receive(:instrument).and_call_original

      client.read_batch_fair("default", qty: 5)

      expect(Pgbus::Instrumentation).to have_received(:instrument)
        .with("pgbus.client.read_batch_fair", queue: "pgbus_test_default", qty: 5)
    end

    it "fails fast with ConnectionCircuitOpenError while the connection breaker is open" do
      allow(client.connection_health).to receive(:run_guarded).and_raise(Pgbus::ConnectionCircuitOpenError)

      expect { client.read_batch_fair("default", qty: 5) }.to raise_error(Pgbus::ConnectionCircuitOpenError)
    end
  end

  describe "#read_batch_prioritized with fair share enabled" do
    before do
      config.fair_share = ->(_job) { "t" }
      allow(raw_conn).to receive(:exec_params).and_return(double("PG::Result", to_a: rows))
    end

    it "delegates the non-priority path to read_batch_fair" do
      result = client.read_batch_prioritized("default", qty: 5)

      expect(mock_pgmq).not_to have_received(:read_batch)
      expect(result.map(&:first).uniq).to eq(["pgbus_test_default"])
      expect(result.map { |(_, m)| m.msg_id }).to eq(%w[7 9])
    end

    context "with priority_levels" do
      before { config.priority_levels = 2 }

      it "fair-reads each sub-queue in order, highest priority first" do
        allow(raw_conn).to receive(:exec_params) do |sql, params|
          if sql.include?("pgbus_test_default_p0")
            double("PG::Result", to_a: [{ "msg_id" => "1" }])
          else
            double("PG::Result", to_a: [{ "msg_id" => "2" }, { "msg_id" => "3" }].first(params[0]))
          end
        end

        result = client.read_batch_prioritized("default", qty: 2)

        expect(result.map(&:first)).to eq(%w[pgbus_test_default_p0 pgbus_test_default_p1])
        expect(result.map { |(_, m)| m.msg_id }).to eq(%w[1 2])
        expect(mock_pgmq).not_to have_received(:read_batch)
      end
    end
  end

  describe "#ensure_fair_index" do
    before { allow(raw_conn).to receive(:exec) }

    it "creates the index concurrently on the prefixed queue table" do
      client.ensure_fair_index("default")

      expect(raw_conn).to have_received(:exec) do |sql|
        expect(sql).to include("CREATE INDEX CONCURRENTLY IF NOT EXISTS q_pgbus_test_default_fair_idx")
        expect(sql).to include("ON pgmq.q_pgbus_test_default")
        expect(sql).to include("pgbus_fair_key")
      end
    end

    it "is memoized per queue per process" do
      client.ensure_fair_index("default")
      client.ensure_fair_index("default")
      client.ensure_fair_index("other")

      expect(raw_conn).to have_received(:exec).twice
    end

    it "treats a duplicate-relation race as success" do
      allow(raw_conn).to receive(:exec) { wrapped_pg_error(PG::DuplicateTable, "relation already exists") }

      expect { client.ensure_fair_index("default") }.not_to raise_error
    end

    it "does not memoize and stays quiet when the queue table does not exist yet" do
      allow(raw_conn).to receive(:exec) { wrapped_pg_error(PG::UndefinedTable, "relation does not exist") }
      allow(Pgbus.logger).to receive(:debug)

      client.ensure_fair_index("default")
      allow(raw_conn).to receive(:exec)
      client.ensure_fair_index("default")

      expect(raw_conn).to have_received(:exec).twice
    end

    it "logs (not raises) other failures with the remediation and does not memoize" do
      allow(raw_conn).to receive(:exec).and_raise(StandardError, "boom")
      logged = []
      allow(Pgbus.logger).to receive(:error) { |&blk| logged << blk.call }

      expect { client.ensure_fair_index("default") }.not_to raise_error
      expect(logged.first).to include("q_pgbus_test_default_fair_idx")
      expect(logged.first).to include("DROP INDEX")
    end
  end

  describe "queue creation" do
    it "creates the fair index (non-concurrently) when fair_share is configured" do
      config.fair_share = ->(_job) { "t" }
      allow(raw_conn).to receive(:exec)

      client.ensure_queue("default")

      expect(raw_conn).to have_received(:exec).with(a_string_including("CREATE INDEX IF NOT EXISTS q_pgbus_test_default_fair_idx"))
    end

    it "does not touch the fair index when fair_share is off" do
      allow(raw_conn).to receive(:exec)

      client.ensure_queue("default")

      expect(raw_conn).not_to have_received(:exec).with(a_string_including("fair_idx"))
    end
  end
end
