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
    # Stub the class method that loads pgmq so the faked PGMQ::Client stands;
    # a clean per-example stub, unlike stubbing global Kernel#require.
    allow(Pgbus::Client).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
    allow(client).to receive(:with_raw_connection).and_yield(raw_conn)
    allow(client).to receive(:ensure_single_queue)
    # Stub notify trigger check — runs raw SQL which needs a real PG connection.
    allow(client).to receive(:notify_trigger_current?).and_return(false)
    allow(Pgbus::StreamQueue).to receive(:record!)
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
    it "creates the BARE queue directly, bypassing the priority strategy" do
      # Must use ensure_single_queue on the bare physical name, NOT
      # ensure_queue (which fans out to the priority strategy's _p0.._pN
      # sub-queues under priority_levels>1, leaving the bare queue — the one
      # the streamer reads and NOTIFYs on — uncreated. Issue #310.
      client.ensure_stream_queue("chat")
      expect(client).to have_received(:ensure_single_queue).with("pgbus_test_chat")
    end

    it "creates the bare queue even when priority_levels > 1" do
      # Build a client whose strategy is PriorityStrategy (memoized at
      # construction), so this genuinely exercises the #310 scenario rather
      # than relying on the bare-name path the default client already takes.
      priority_config = Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
        c.priority_levels = 3
      end
      priority_client = Pgbus::Client.new(priority_config)
      priority_client.instance_variable_set(:@schema_ensured, true)
      allow(priority_client).to receive_messages(
        with_raw_connection: nil, ensure_single_queue: nil, notify_trigger_current?: false
      )
      allow(priority_client).to receive(:with_raw_connection).and_yield(raw_conn)

      priority_client.ensure_stream_queue("chat")

      expect(priority_client).to have_received(:ensure_single_queue).with("pgbus_test_chat")
      expect(priority_client).not_to have_received(:ensure_single_queue).with(a_string_matching(/_p\d+$/))
    end

    it "records the physical queue name in the StreamQueue registry" do
      client.ensure_stream_queue("chat")

      expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat")
    end

    it "records the registry entry only once per stream per process" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("chat")

      expect(Pgbus::StreamQueue).to have_received(:record!).with("pgbus_test_chat").once
    end

    it "creates the msg_id index on the archive table" do
      client.ensure_stream_queue("chat")

      expect(raw_conn).to have_received(:exec)
        .with(a_string_matching(/CREATE INDEX IF NOT EXISTS\s+a_pgbus_test_chat_msg_id_idx\s+ON pgmq\.a_pgbus_test_chat\s*\(msg_id\)/m))
    end

    it "is idempotent — calling twice skips the second CREATE INDEX via per-process memoization" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("chat")

      # ensure_single_queue still runs twice — its own memoization is on a
      # different layer (@queues_created) and this spec mocks it out.
      # The CREATE INDEX SQL only runs on the first call because we
      # now memoize on @stream_indexes_created.
      expect(raw_conn).to have_received(:exec).once
      expect(client).to have_received(:ensure_single_queue).twice
    end

    it "memoizes index creation per stream, not globally" do
      allow(raw_conn).to receive(:exec)

      client.ensure_stream_queue("chat")
      client.ensure_stream_queue("orders")

      expect(raw_conn).to have_received(:exec).twice
    end

    it "normalizes stream names with invalid characters via QueueNameValidator" do
      received_sql = nil
      allow(raw_conn).to receive(:exec) do |sql|
        received_sql = sql
      end

      expect { client.ensure_stream_queue("nope; DROP TABLE") }.not_to raise_error
      expect(received_sql).to match(/pgmq\.a_pgbus_test_nopeDROPTABLE/i)
      expect(received_sql).not_to include(";")
    end

    # Deploy-time thundering herd (issue #403): two processes with cold memos
    # ensure the same lazy stream queue concurrently; the loser's CREATE
    # CONSTRAINT TRIGGER inside pgmq.enable_notify_insert fails with
    # PG::DuplicateObject once the winner commits. The trigger provably
    # exists, so the broadcast must proceed instead of dropping.
    context "when a concurrent ensure won the NOTIFY trigger race" do
      it "treats the duplicate-trigger error as success" do
        real_pgmq_connection_error
        allow(client).to receive(:notify_trigger_current?).and_return(false, true)
        allow(mock_pgmq).to receive(:enable_notify_insert).and_raise(
          PGMQ::Errors::ConnectionError,
          'Database connection error: ERROR:  trigger "trigger_notify_queue_insert_listeners" ' \
          'for relation "q_pgbus_test_chat" already exists'
        )

        expect { client.ensure_stream_queue("chat") }.not_to raise_error
        expect(mock_pgmq).to have_received(:enable_notify_insert)
          .with("pgbus_test_chat", throttle_interval_ms: 0).once
      end
    end

    # Stale process-local memo after another process drops the physical queue
    # (orphan stream sweep, dashboard drop, manual drop_queue). Without recovery
    # enable_notify_insert raises "Queue does not exist" even though ensure
    # already ran — the memo skipped pgmq.create.
    context "when the process-local queue memo is stale" do
      let(:full_name) { "pgbus_test_chat" }
      let(:missing_queue_message) do
        'Queue "pgbus_test_chat" does not exist. Create it first using pgmq.create()'
      end

      before do
        real_pgmq_connection_error
        client.instance_variable_get(:@queues_created)[full_name] = true
        client.instance_variable_get(:@stream_indexes_created)["chat"] = true
      end

      it "clears the memo and recreates the queue when enable_notify reports it missing" do
        calls = 0
        allow(mock_pgmq).to receive(:enable_notify_insert) do
          calls += 1
          raise PGMQ::Errors::ConnectionError, missing_queue_message if calls == 1

          nil
        end

        expect { client.ensure_stream_queue("chat") }.not_to raise_error

        expect(client).to have_received(:ensure_single_queue).with(full_name).twice
        expect(client.instance_variable_get(:@queues_created)[full_name]).to be_nil
        expect(client.instance_variable_get(:@stream_indexes_created)["chat"]).to be(true)
        expect(calls).to eq(2)
      end

      it "re-raises unrelated ConnectionErrors without clearing the memo or retrying" do
        # Use a non-stale ConnectionError so with_stale_connection_retry does not
        # wrap this path (PQsocket / server-closed match the stale patterns and
        # would re-enter ensure_single_queue on their own).
        allow(mock_pgmq).to receive(:enable_notify_insert)
          .and_raise(PGMQ::Errors::ConnectionError, "permission denied for schema pgmq")

        expect { client.ensure_stream_queue("chat") }
          .to raise_error(PGMQ::Errors::ConnectionError, /permission denied/)

        expect(client).to have_received(:ensure_single_queue).with(full_name).once
        expect(client.instance_variable_get(:@queues_created)[full_name]).to be(true)
      end
    end
  end
end
