# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::NotifyStream do
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
    allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
    end
  end
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn) { double("raw_conn", exec_params: nil) }

  describe "#notify_stream" do
    it "sends a PG NOTIFY on the PGMQ channel for the stream" do
      client.notify_stream("chat", { "html" => "<div>hello</div>" })

      expect(raw_conn).to have_received(:exec_params).with(
        a_string_matching(/SELECT pg_notify/),
        a_collection_including(
          a_string_matching(/pgmq\.q_pgbus_test_chat\.INSERT/),
          a_string_matching(/"html"/)
        )
      )
    end

    it "does not create a PGMQ queue" do
      client.notify_stream("chat", { "html" => "X" })
      expect(mock_pgmq).not_to have_received(:create)
    end

    it "JSON-serializes the payload in the NOTIFY" do
      payload = { "html" => "<div>test</div>", "visible_to" => "admin" }
      client.notify_stream("chat", payload)

      expect(raw_conn).to have_received(:exec_params).with(
        anything,
        a_collection_including(anything, JSON.generate(payload))
      )
    end

    it "sanitizes the stream name for the NOTIFY channel" do
      client.notify_stream("nope; DROP TABLE", { "html" => "X" })

      expect(raw_conn).to have_received(:exec_params).with(
        anything,
        a_collection_including(
          a_string_matching(/pgbus_test_nopeDROPTABLE/i),
          anything
        )
      )
    end

    context "with stale pgmq connection recovery" do
      before do
        real_pgmq_connection_error
        # Stale retries now back off between attempts; stub the delay so the
        # suite doesn't actually sleep.
        allow(client).to receive(:sleep)
      end

      let(:ssl_eof_msg) { "Database connection error: PQconsumeInput() SSL error: unexpected eof while reading" }

      it "retries once on an idle-socket SSL EOF" do
        call_count = 0
        allow(mock_pgmq).to receive(:with_connection) do |&block|
          call_count += 1
          raise PGMQ::Errors::ConnectionError, ssl_eof_msg if call_count == 1

          block.call(raw_conn)
        end

        expect do
          client.notify_stream("chat", { "html" => "<turbo-stream/>" })
        end.not_to raise_error

        expect(call_count).to eq(2)
      end

      it "does not retry on a non-matching ConnectionError" do
        allow(mock_pgmq).to receive(:with_connection)
          .and_raise(PGMQ::Errors::ConnectionError, "Connection pool timeout: waited 5.00s")

        expect do
          client.notify_stream("chat", { "html" => "X" })
        end.to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/)
      end

      it "gives up after the maximum retries (persistent failure) and re-raises" do
        call_count = 0
        allow(mock_pgmq).to receive(:with_connection) do |&_block|
          call_count += 1
          raise PGMQ::Errors::ConnectionError, ssl_eof_msg
        end

        expect do
          client.notify_stream("chat", { "html" => "X" })
        end.to raise_error(PGMQ::Errors::ConnectionError)

        # Initial attempt + 2 retries = 3 calls.
        expect(call_count).to eq(3)
        expect(client).to have_received(:sleep).with(0.1).ordered
        expect(client).to have_received(:sleep).with(0.5).ordered
      end

      it "logs a warning on the retry path" do
        call_count = 0
        allow(mock_pgmq).to receive(:with_connection) do |&block|
          call_count += 1
          raise PGMQ::Errors::ConnectionError, ssl_eof_msg if call_count == 1

          block.call(raw_conn)
        end

        allow(Pgbus.logger).to receive(:warn)

        client.notify_stream("chat", { "html" => "X" })

        expect(Pgbus.logger).to have_received(:warn)
      end
    end
  end
end
