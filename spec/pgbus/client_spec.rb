# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client do
  # Stub pgmq-ruby so it never loads the real gem
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = described_class.new(config)
    # Pre-mark PGMQ schema as ensured for most tests.
    # Schema installation tests override this.
    c.instance_variable_set(:@schema_ensured, true)
    c
  end

  before do
    allow_any_instance_of(described_class).to receive(:require).with("pgmq").and_return(true)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }

  describe "#ensure_pgmq_schema (via ensure_queue)" do
    let(:raw_conn) { double("raw_conn") }

    before do
      client.instance_variable_set(:@schema_ensured, false)
      allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
    end

    it "installs via embedded SQL when pgmq.meta missing and no extension (auto mode)" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql).and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
    end

    it "installs via extension when available in auto mode" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 1))
      allow(raw_conn).to receive(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq").and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq")
    end

    it "respects :extension schema mode" do
      config.pgmq_schema_mode = :extension
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq").and_return(nil)

      client.ensure_queue("jobs")

      expect(raw_conn).to have_received(:exec).with("CREATE EXTENSION IF NOT EXISTS pgmq")
      expect(raw_conn).not_to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
      config.pgmq_schema_mode = :auto
    end

    it "skips installation when pgmq.meta already exists" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))

      client.ensure_queue("jobs")

      expect(raw_conn).not_to have_received(:exec).with(Pgbus::PgmqSchema.install_sql)
    end

    it "wraps install failures as SchemaNotReady" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(/pg_available_extensions/).and_return(double(ntuples: 0))
      allow(raw_conn).to receive(:exec).with(Pgbus::PgmqSchema.install_sql)
                     .and_raise(StandardError, "permission denied for schema pgmq")

      expect { client.ensure_queue("jobs") }.to raise_error(
        Pgbus::SchemaNotReady, /PGMQ schema installation failed/
      )
    end

    it "only checks once per client instance" do
      allow(raw_conn).to receive(:exec).with(/pg_tables.*pgmq.*meta/).and_return(double(ntuples: 1))

      client.ensure_queue("jobs")
      client.ensure_queue("events")

      expect(raw_conn).to have_received(:exec).with(/pg_tables.*pgmq.*meta/).once
    end
  end

  describe "#ensure_queue" do
    it "creates the queue with the prefixed name" do
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs")
    end

    it "enables LISTEN/NOTIFY when listen_notify is true" do
      config.listen_notify = true
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:enable_notify_insert).with("pgbus_test_jobs", throttle_interval_ms: config.notify_throttle_ms)
    end

    it "skips LISTEN/NOTIFY when listen_notify is false" do
      config.listen_notify = false
      client.ensure_queue("jobs")

      expect(mock_pgmq).not_to have_received(:enable_notify_insert)
    end

    it "is idempotent — only creates the queue once" do
      client.ensure_queue("jobs")
      client.ensure_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").once
    end

    it "creates different queues independently" do
      client.ensure_queue("jobs")
      client.ensure_queue("events")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs").once
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_events").once
    end

    it "propagates PGMQ connection errors when queue creation fails" do
      stub_const("PGMQ::Errors::ConnectionError", Class.new(StandardError)) unless defined?(PGMQ::Errors::ConnectionError)

      allow(mock_pgmq).to receive(:create).and_raise(
        PGMQ::Errors::ConnectionError.new("Database connection error: connection refused")
      )

      expect { client.ensure_queue("jobs") }.to raise_error(PGMQ::Errors::ConnectionError)
    end
  end

  describe "#ensure_dead_letter_queue" do
    it "creates the DLQ with correct suffix" do
      client.ensure_dead_letter_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs_dlq")
    end

    it "is idempotent — only creates the DLQ once" do
      client.ensure_dead_letter_queue("jobs")
      client.ensure_dead_letter_queue("jobs")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_jobs_dlq").once
    end
  end

  describe "#send_message" do
    it "ensures the queue exists before sending" do
      client.send_message("default", { "type" => "test" })

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
    end

    it "produces a JSON-serialized message to the prefixed queue" do
      client.send_message("default", { "key" => "value" })

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        '{"key":"value"}',
        headers: nil,
        delay: 0
      )
    end

    it "passes headers and delay" do
      client.send_message("default", "payload", headers: { "x" => 1 }, delay: 5)

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        "payload",
        headers: '{"x":1}',
        delay: 5
      )
    end

    it "does not double-serialize a string payload" do
      client.send_message("default", '{"already":"json"}')

      expect(mock_pgmq).to have_received(:produce).with(
        "pgbus_test_default",
        '{"already":"json"}',
        headers: nil,
        delay: 0
      )
    end
  end

  describe "#send_batch" do
    it "produces a batch of serialized messages" do
      payloads = [{ "a" => 1 }, { "b" => 2 }]
      client.send_batch("default", payloads)

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        ['{"a":1}', '{"b":2}'],
        headers: nil,
        delay: 0
      )
    end

    it "serializes headers when provided" do
      client.send_batch("default", ["p1"], headers: [{ "h" => 1 }])

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        ["p1"],
        headers: ['{"h":1}'],
        delay: 0
      )
    end

    it "preserves nil elements in mixed headers arrays" do
      client.send_batch("default", %w[p1 p2], headers: [nil, { "h" => 1 }])

      expect(mock_pgmq).to have_received(:produce_batch).with(
        "pgbus_test_default",
        %w[p1 p2],
        headers: [nil, '{"h":1}'],
        delay: 0
      )
    end
  end

  describe "#read_message" do
    it "reads from the prefixed queue with default visibility timeout" do
      client.read_message("default")

      expect(mock_pgmq).to have_received(:read).with("pgbus_test_default", vt: config.visibility_timeout)
    end

    it "allows overriding the visibility timeout" do
      client.read_message("default", vt: 60)

      expect(mock_pgmq).to have_received(:read).with("pgbus_test_default", vt: 60)
    end
  end

  describe "#read_batch" do
    it "reads a batch from the prefixed queue" do
      client.read_batch("default", qty: 10)

      expect(mock_pgmq).to have_received(:read_batch).with("pgbus_test_default", vt: config.visibility_timeout, qty: 10)
    end
  end

  describe "#read_multi" do
    it "reads from multiple prefixed queues in a single call" do
      client.read_multi(%w[default urgent], qty: 10)

      expect(mock_pgmq).to have_received(:read_multi).with(
        %w[pgbus_test_default pgbus_test_urgent],
        vt: config.visibility_timeout,
        qty: 10
      )
    end

    it "allows overriding the visibility timeout" do
      client.read_multi(%w[default], qty: 5, vt: 60)

      expect(mock_pgmq).to have_received(:read_multi).with(
        %w[pgbus_test_default],
        vt: 60,
        qty: 5
      )
    end
  end

  describe "#read_with_poll" do
    it "delegates to pgmq.read_with_poll with correct args" do
      client.read_with_poll("default", qty: 5, max_poll_seconds: 2, poll_interval_ms: 50)

      expect(mock_pgmq).to have_received(:read_with_poll).with(
        "pgbus_test_default",
        vt: config.visibility_timeout,
        qty: 5,
        max_poll_seconds: 2,
        poll_interval_ms: 50
      )
    end
  end

  describe "#delete_message" do
    it "deletes from the prefixed queue by default" do
      client.delete_message("default", 42)

      expect(mock_pgmq).to have_received(:delete).with("pgbus_test_default", 42)
    end

    it "skips prefix when prefixed: false" do
      client.delete_message("raw_queue", 99, prefixed: false)

      expect(mock_pgmq).to have_received(:delete).with("raw_queue", 99)
    end
  end

  describe "#archive_message" do
    it "archives from the prefixed queue by default" do
      client.archive_message("default", 7)

      expect(mock_pgmq).to have_received(:archive).with("pgbus_test_default", 7)
    end

    it "skips prefix when prefixed: false" do
      client.archive_message("pgbus_test_default_p0", 42, prefixed: false)

      expect(mock_pgmq).to have_received(:archive).with("pgbus_test_default_p0", 42)
    end
  end

  describe "#archive_batch" do
    it "archives multiple messages from the prefixed queue" do
      client.archive_batch("default", [1, 2, 3])

      expect(mock_pgmq).to have_received(:archive_batch).with("pgbus_test_default", [1, 2, 3])
    end

    it "skips prefix when prefixed: false" do
      client.archive_batch("raw_queue", [4, 5], prefixed: false)

      expect(mock_pgmq).to have_received(:archive_batch).with("raw_queue", [4, 5])
    end
  end

  describe "#delete_batch" do
    it "deletes multiple messages from the prefixed queue" do
      client.delete_batch("default", [1, 2])

      expect(mock_pgmq).to have_received(:delete_batch).with("pgbus_test_default", [1, 2])
    end

    it "skips prefix when prefixed: false" do
      client.delete_batch("pgbus_test_default_dlq", [4, 5], prefixed: false)

      expect(mock_pgmq).to have_received(:delete_batch).with("pgbus_test_default_dlq", [4, 5])
    end
  end

  describe "#set_visibility_timeout" do
    it "adds prefix by default" do
      client.set_visibility_timeout("default", 5, vt: 120)

      expect(mock_pgmq).to have_received(:set_vt).with("pgbus_test_default", 5, vt: 120)
    end

    it "skips prefix when prefixed: false" do
      client.set_visibility_timeout("raw_queue", 3, vt: 60, prefixed: false)

      expect(mock_pgmq).to have_received(:set_vt).with("raw_queue", 3, vt: 60)
    end
  end

  describe "#transaction" do
    it "delegates to pgmq.transaction" do
      yielded = false
      client.transaction { |_txn| yielded = true }

      expect(yielded).to be true
    end
  end

  describe "#move_to_dead_letter" do
    it "ensures the DLQ exists" do
      message = build_message_double(msg_id: 42, message: '{"data":"test"}', headers: nil)
      client.move_to_dead_letter("default", message)

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_dlq")
    end

    it "produces to DLQ and deletes from the original queue within a transaction" do
      message = build_message_double(msg_id: 42, message: '{"data":"test"}', headers: nil)
      client.move_to_dead_letter("default", message)

      expect(mock_pgmq).to have_received(:transaction)
      expect(mock_pgmq).to have_received(:produce).with("pgbus_test_default_dlq", '{"data":"test"}', headers: nil)
      expect(mock_pgmq).to have_received(:delete).with("pgbus_test_default", 42)
    end
  end

  describe "#metrics" do
    context "with a queue_name" do
      it "returns metrics for the prefixed queue" do
        client.metrics("default")

        expect(mock_pgmq).to have_received(:metrics).with("pgbus_test_default")
      end
    end

    context "without a queue_name" do
      it "returns all metrics" do
        client.metrics

        expect(mock_pgmq).to have_received(:metrics_all)
      end
    end
  end

  describe "#list_queues" do
    it "delegates to pgmq.list_queues" do
      client.list_queues

      expect(mock_pgmq).to have_received(:list_queues)
    end
  end

  describe "#purge_queue" do
    it "purges the prefixed queue" do
      client.purge_queue("default")

      expect(mock_pgmq).to have_received(:purge_queue).with("pgbus_test_default")
    end

    it "skips prefixing when prefixed: false" do
      client.purge_queue("pgbus_test_default", prefixed: false)

      expect(mock_pgmq).to have_received(:purge_queue).with("pgbus_test_default")
    end
  end

  describe "#drop_queue" do
    it "drops the prefixed queue" do
      client.drop_queue("default")

      expect(mock_pgmq).to have_received(:drop_queue).with("pgbus_test_default")
    end

    it "skips prefixing when prefixed: false" do
      client.drop_queue("pgbus_test_default", prefixed: false)

      expect(mock_pgmq).to have_received(:drop_queue).with("pgbus_test_default")
    end

    it "removes the queue from the created cache" do
      client.ensure_queue("default")
      client.drop_queue("default")

      # Verify the queue is no longer in the cache
      expect(client.instance_variable_get(:@queues_created)["pgbus_test_default"]).to be_nil
    end
  end

  describe "#purge_archive" do
    let(:conn) { double("conn") }
    let(:result) { double("result", cmd_tuples: 50) }

    before do
      allow(client).to receive(:with_raw_connection).and_yield(conn)
      allow(conn).to receive(:exec_params).and_return(result)
    end

    it "deletes archive entries older than the given time" do
      cutoff = Time.now - 3600
      client.purge_archive("default", older_than: cutoff)

      expect(conn).to have_received(:exec_params).with(
        a_string_matching(/DELETE FROM pgmq\.a_pgbus_test_default/),
        [cutoff, 1000]
      )
    end

    it "loops until batch is not full" do
      small_result = double("result", cmd_tuples: 10)
      allow(conn).to receive(:exec_params).and_return(result, small_result)

      total = client.purge_archive("default", older_than: Time.now, batch_size: 50)
      expect(total).to eq(60) # 50 + 10
    end
  end

  describe "#message_exists?" do
    let(:conn) { double("conn") }

    before { allow(client).to receive(:with_raw_connection).and_yield(conn) }

    it "raises ArgumentError when neither msg_id nor uniqueness_key is given" do
      expect { client.message_exists?("default") }.to raise_error(ArgumentError)
    end

    context "when looking up by msg_id" do
      it "returns true when the row exists in the prefixed queue table" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/SELECT 1 FROM pgmq\.q_pgbus_test_default WHERE msg_id = \$1/),
          [42]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("default", msg_id: 42)).to be(true)
      end

      it "returns false when the row does not exist" do
        allow(conn).to receive(:exec_params).and_return(double("result", ntuples: 0))

        expect(client.message_exists?("default", msg_id: 42)).to be(false)
      end

      it "accepts an already-prefixed physical queue name" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/pgmq\.q_pgbus_test_default/),
          [42]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("pgbus_test_default", msg_id: 42)).to be(true)
      end
    end

    context "when looking up by uniqueness_key" do
      it "queries the JSONB pgbus_uniqueness_key field" do
        allow(conn).to receive(:exec_params).with(
          a_string_matching(/message::jsonb ->> 'pgbus_uniqueness_key' = \$1/),
          ["MyJob"]
        ).and_return(double("result", ntuples: 1))

        expect(client.message_exists?("default", uniqueness_key: "MyJob")).to be(true)
      end
    end

    context "when the queue table is missing" do
      before { stub_const("PG::UndefinedTable", Class.new(StandardError)) }

      it "returns nil when the cause is PG::UndefinedTable (locale-independent)" do
        cause = PG::UndefinedTable.new("relation does not exist")
        wrapped = ActiveRecord::StatementInvalid.new("ERROR: relation \"pgmq.q_x\" does not exist")
        allow(wrapped).to receive(:cause).and_return(cause)
        allow(conn).to receive(:exec_params).and_raise(wrapped)

        expect(client.message_exists?("nonexistent", msg_id: 1)).to be_nil
      end

      it "re-raises a StatementInvalid when it is not an UndefinedTable" do
        wrapped = ActiveRecord::StatementInvalid.new("syntax error")
        allow(wrapped).to receive(:cause).and_return(StandardError.new("syntax error"))
        allow(conn).to receive(:exec_params).and_raise(wrapped)

        expect { client.message_exists?("default", msg_id: 1) }.to raise_error(ActiveRecord::StatementInvalid)
      end
    end
  end

  describe "#read_batch_prioritized" do
    context "when priority is not enabled" do
      it "falls back to regular read_batch" do
        client.read_batch_prioritized("default", qty: 5)

        expect(mock_pgmq).to have_received(:read_batch).with("pgbus_test_default", vt: config.visibility_timeout, qty: 5)
      end
    end

    context "when priority is enabled" do
      before { config.priority_levels = 3 }
      after { config.priority_levels = nil }

      it "reads from p0 first, then p1, then p2" do
        # Ensure all sub-queues are created first
        client.ensure_queue("default")

        msg0 = build_message_double(msg_id: 1, message: '{"p":0}')
        msg1 = build_message_double(msg_id: 2, message: '{"p":1}')

        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p0", anything).and_return([msg0])
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p1", anything).and_return([msg1])
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p2", anything).and_return([])

        results = client.read_batch_prioritized("default", qty: 5)

        expect(results.size).to eq(2)
        expect(results[0][0]).to eq("pgbus_test_default_p0")
        expect(results[1][0]).to eq("pgbus_test_default_p1")
      end

      it "stops when qty is filled" do
        client.ensure_queue("default")

        msgs = 3.times.map { |i| build_message_double(msg_id: i, message: "{}") }
        allow(mock_pgmq).to receive(:read_batch)
          .with("pgbus_test_default_p0", anything).and_return(msgs)

        results = client.read_batch_prioritized("default", qty: 3)

        expect(results.size).to eq(3)
        # Should not read from p1 or p2
        expect(mock_pgmq).not_to have_received(:read_batch).with("pgbus_test_default_p1", anything)
      end
    end
  end

  describe "#send_message with priority" do
    context "when priority is enabled" do
      before { config.priority_levels = 3 }
      after { config.priority_levels = nil }

      it "routes to the correct sub-queue" do
        client.send_message("default", { "data" => "test" }, priority: 0)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p0",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end

      it "uses default_priority when none specified" do
        config.default_priority = 1
        client.send_message("default", { "data" => "test" })

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p1",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end

      it "clamps priority to valid range" do
        client.send_message("default", { "data" => "test" }, priority: 99)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default_p2",
          '{"data":"test"}',
          headers: nil,
          delay: 0
        )
      end
    end
  end

  describe "#bind_topic" do
    it "ensures the queue and binds the pattern" do
      client.bind_topic("orders.#", "events")

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_events")
      expect(mock_pgmq).to have_received(:bind_topic).with("orders.#", "pgbus_test_events")
    end
  end

  describe "#publish_to_topic" do
    it "publishes a serialized payload with the routing key" do
      client.publish_to_topic("orders.created", { "id" => 1 })

      expect(mock_pgmq).to have_received(:produce_topic).with(
        "orders.created",
        '{"id":1}',
        headers: nil,
        delay: 0
      )
    end

    it "serializes headers when provided" do
      client.publish_to_topic("orders.created", "body", headers: { "trace" => "abc" }, delay: 3)

      expect(mock_pgmq).to have_received(:produce_topic).with(
        "orders.created",
        "body",
        headers: '{"trace":"abc"}',
        delay: 3
      )
    end
  end

  describe "#ensure_all_queues" do
    it "creates the default queue" do
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
    end

    it "creates all queues from worker configs" do
      config.workers = [
        { queues: %w[default urgent], threads: 5 },
        { queues: %w[emails], threads: 2 }
      ]
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_urgent")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_emails")
    end

    it "creates queues for recurring tasks" do
      config.recurring_tasks = {
        "daily_report" => { class: "ReportJob", schedule: "0 2 * * *", queue: "reports" }
      }
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_reports")
    end

    it "deduplicates queues" do
      config.workers = [
        { queues: %w[default], threads: 5 },
        { queues: %w[default], threads: 3 }
      ]
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default").once
    end

    it "skips wildcard queue entries" do
      config.workers = [{ queues: %w[*], threads: 5 }]
      client.ensure_all_queues

      # Should still create default queue but not try to create "*"
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default")
      expect(mock_pgmq).not_to have_received(:create).with("pgbus_test_*")
    end

    it "creates priority sub-queues when priority is enabled" do
      config.priority_levels = 3
      client.ensure_all_queues

      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p0")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p1")
      expect(mock_pgmq).to have_received(:create).with("pgbus_test_default_p2")
      config.priority_levels = nil
    end

    it "logs the bootstrapped queues" do
      allow(Pgbus.logger).to receive(:info)
      client.ensure_all_queues

      expect(Pgbus.logger).to have_received(:info).at_least(:once)
    end
  end

  describe "#close" do
    it "closes the pgmq connection" do
      client.close

      expect(mock_pgmq).to have_received(:close)
    end
  end

  describe "connection pooling strategy" do
    it "uses configured pool_size when database_url is set (dedicated connections)" do
      url_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(url_config)

      expect(PGMQ::Client).to have_received(:new).with(
        "postgres://localhost/pgbus_test",
        pool_size: 5,
        pool_timeout: url_config.pool_timeout
      )
    end

    it "uses configured pool_size when connection_params is set (dedicated connections)" do
      url_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = { host: "localhost", dbname: "pgbus_test" }
        c.pool_size = 3
        c.queue_prefix = "pgbus_test"
      end

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(url_config)

      expect(PGMQ::Client).to have_received(:new).with(
        { host: "localhost", dbname: "pgbus_test" },
        pool_size: 3,
        pool_timeout: url_config.pool_timeout
      )
    end

    it "forces pool_size=1 when connection_options falls back to Proc (shared connection)" do
      lambda_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      # Simulate the Rails path where AR config extraction fails, falling back to Proc
      raw_conn = double("PG::Connection")
      ar_connection = double("AR::ConnectionAdapter", raw_connection: raw_conn)
      ar_base = double("AR::Base", connection: ar_connection)
      allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
      stub_const("ActiveRecord::Base", ar_base)

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      described_class.new(lambda_config)

      expect(PGMQ::Client).to have_received(:new).with(
        an_instance_of(Proc),
        pool_size: 1,
        pool_timeout: lambda_config.pool_timeout
      )
    end

    it "does not use mutex synchronization for dedicated connections" do
      config.database_url = "postgres://localhost/pgbus_test"
      config.pool_size = 5

      # With dedicated connections, operations should not go through the mutex.
      # We test this by verifying that the client does not have a @pgmq_mutex.
      expect(client.instance_variable_get(:@pgmq_mutex)).to be_nil
    end

    it "uses mutex synchronization when falling back to shared (Proc) connections" do
      lambda_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = nil
        c.connection_params = nil
        c.pool_size = 5
        c.queue_prefix = "pgbus_test"
      end

      raw_conn = double("PG::Connection")
      ar_connection = double("AR::ConnectionAdapter", raw_connection: raw_conn)
      ar_base = double("AR::Base", connection: ar_connection)
      allow(ar_base).to receive(:connection_db_config).and_raise(StandardError, "no config")
      stub_const("ActiveRecord::Base", ar_base)

      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      shared_client = described_class.new(lambda_config)
      shared_client.instance_variable_set(:@schema_ensured, true)

      expect(shared_client.instance_variable_get(:@pgmq_mutex)).to be_a(Mutex)
    end
  end
end
