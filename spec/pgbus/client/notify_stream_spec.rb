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
  end
end
