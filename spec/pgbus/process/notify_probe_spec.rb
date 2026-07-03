# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::NotifyProbe do
  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  let(:logger) { instance_double(Logger, error: nil, warn: nil, debug: nil, info: nil) }

  # A minimal PG::Connection double whose wait_for_notify behavior is
  # configurable per scenario. It records every SQL string passed to exec so
  # tests can assert LISTEN/UNLISTEN issuance.
  def build_conn(deliver:, notify_raises: nil)
    executed = []
    conn = Object.new
    conn.define_singleton_method(:exec) do |sql|
      executed << sql
      nil
    end
    conn.define_singleton_method(:exec_params) do |sql, _params|
      executed << sql
      raise notify_raises if notify_raises

      nil
    end
    conn.define_singleton_method(:wait_for_notify) do |_timeout, &block|
      # deliver == false → timeout → returns nil
      next nil unless deliver

      # Emulate PG delivering the notification on the same session.
      channel = executed.grep(/\ALISTEN /).last.to_s[/LISTEN "(.+)"/, 1]
      block&.call(channel, 0, "")
      channel
    end
    conn.define_singleton_method(:executed) { executed }
    conn
  end

  describe ".probe_notify_delivery!" do
    context "when the self-NOTIFY is delivered (healthy direct connection)" do
      let(:conn) { build_conn(deliver: true) }

      it "returns true" do
        expect(described_class.probe_notify_delivery!(conn, logger: logger)).to be true
      end

      it "does not log an error" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        expect(logger).not_to have_received(:error)
      end

      it "leaves no lingering probe LISTEN registration (issues UNLISTEN)" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        listen = conn.executed.grep(/\ALISTEN /).first
        channel = listen[/LISTEN "(.+)"/, 1]
        expect(conn.executed).to include(%(UNLISTEN "#{channel}"))
      end

      it "uses a probe channel scoped to the process id" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        listen = conn.executed.grep(/\ALISTEN /).first
        expect(listen).to match(/LISTEN "pgbus_probe_#{Process.pid}_[0-9a-f]+"/)
      end
    end

    context "when the self-NOTIFY never arrives (transaction-pooled PgBouncer)" do
      let(:conn) { build_conn(deliver: false) }

      it "returns false" do
        expect(described_class.probe_notify_delivery!(conn, logger: logger)).to be false
      end

      it "logs an actionable error naming both override families" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        expect(logger).to have_received(:error) do |&block|
          message = block.call
          expect(message).to match(/worker_notify_database_url/)
          expect(message).to match(/worker_notify_host/)
          expect(message).to match(/worker_notify_port/)
          expect(message).to match(/streams_database_url/)
          expect(message).to match(/streams_host/)
          expect(message).to match(/streams_port/)
        end
      end

      it "best-effort UNLISTENs the probe channel even on failure" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        expect(conn.executed.grep(/\AUNLISTEN /)).not_to be_empty
      end
    end

    context "when pg_notify raises (read-only replica)" do
      let(:conn) { build_conn(deliver: false, notify_raises: PG::Error.new("cannot execute pg_notify() in a read-only transaction")) }

      it "treats the raise as a probe failure and returns false" do
        expect(described_class.probe_notify_delivery!(conn, logger: logger)).to be false
      end

      it "logs an actionable error" do
        described_class.probe_notify_delivery!(conn, logger: logger)
        expect(logger).to have_received(:error)
      end

      it "does not let the error escape" do
        expect { described_class.probe_notify_delivery!(conn, logger: logger) }.not_to raise_error
      end
    end

    context "when UNLISTEN itself raises during cleanup" do
      it "still returns the probe result without raising" do
        conn = build_conn(deliver: true)
        # Make UNLISTEN raise; the probe must swallow cleanup errors.
        original = conn.method(:exec)
        conn.define_singleton_method(:exec) do |sql|
          raise PG::Error, "unlisten boom" if sql.start_with?("UNLISTEN")

          original.call(sql)
        end

        expect(described_class.probe_notify_delivery!(conn, logger: logger)).to be true
      end
    end
  end
end
