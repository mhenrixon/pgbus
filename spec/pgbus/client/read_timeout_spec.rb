# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client do
  describe "read_timeout" do
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      c = described_class.new(config)
      c.instance_variable_set(:@schema_ensured, true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    let(:mock_pgmq) { build_mock_pgmq }
    let(:config) do
      Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
      end
    end

    before do
      allow_any_instance_of(described_class).to receive(:require).with("pgmq").and_return(true)
      stub_const("PGMQ::Client", Class.new { def initialize(*args, **kwargs); end })
      stub_const("PGMQ::Errors", Module.new)
      stub_const("PGMQ::Errors::ConnectionError", Class.new(StandardError))
      # pg is loaded via pgmq-ruby in production; stub the one method the
      # read-bounds gate consults so client construction works with mocked PGMQ.
      stub_pg_library_version
    end

    after do
      config.read_timeout = 30
    end

    describe "#with_read_timeout" do
      # The primary read bounds are libpq-native (statement_timeout +
      # tcp_user_timeout), baked into the connection. The Ruby Timeout is a
      # narrow fallback used ONLY on a dedicated connection where libpq can't
      # bound the socket (non-Linux / libpq < 12). To exercise it deterministically
      # regardless of the host, force @libpq_read_bounds_effective false.
      context "when libpq cannot bound the socket (Ruby Timeout fallback active)" do
        before do
          config.read_timeout = 1
          client.instance_variable_set(:@libpq_read_bounds_effective, false)
        end

        it "raises ReadTimeoutError when read_batch exceeds the timeout" do
          allow(mock_pgmq).to receive(:read_batch) { sleep 10 }

          expect { client.read_batch("default", qty: 5) }.to raise_error(Pgbus::ReadTimeoutError)
        end

        it "raises ReadTimeoutError when read_multi exceeds the timeout" do
          allow(mock_pgmq).to receive(:read_multi) { sleep 10 }

          expect { client.read_multi(%w[default], qty: 5) }.to raise_error(Pgbus::ReadTimeoutError)
        end

        it "raises ReadTimeoutError when read_message exceeds the timeout" do
          allow(mock_pgmq).to receive(:read) { sleep 10 }

          expect { client.read_message("default") }.to raise_error(Pgbus::ReadTimeoutError)
        end

        it "does not interfere with fast reads" do
          allow(mock_pgmq).to receive(:read_batch).and_return([])

          expect(client.read_batch("default", qty: 5)).to eq([])
        end
      end

      # On a dedicated connection where libpq's bounds are effective (Linux,
      # read_timeout set, libpq >= 12), Ruby Timeout is never wired in — reads
      # rely purely on statement_timeout + tcp_user_timeout.
      context "when libpq bounds are effective (no Ruby Timeout)" do
        before do
          config.read_timeout = 1
          client.instance_variable_set(:@libpq_read_bounds_effective, true)
        end

        it "does NOT wrap the read in Ruby Timeout" do
          allow(mock_pgmq).to receive(:read_batch).and_return([])
          allow(Timeout).to receive(:timeout).and_call_original

          client.read_batch("default", qty: 5)

          expect(Timeout).not_to have_received(:timeout)
        end

        it "still maps a server-side statement_timeout cancellation to ReadTimeoutError" do
          allow(mock_pgmq).to receive(:read_batch)
            .and_raise(PGMQ::Errors::ConnectionError, "canceling statement due to statement timeout")

          expect { client.read_batch("default", qty: 5) }.to raise_error(Pgbus::ReadTimeoutError)
        end
      end

      context "when read_timeout is nil" do
        before { config.read_timeout = nil }

        it "does not wrap reads in a timeout" do
          allow(mock_pgmq).to receive(:read_batch).and_return([])

          expect(client.read_batch("default", qty: 5)).to eq([])
        end
      end

      # The shared-AR (Proc) path never uses Ruby Timeout — pgbus doesn't own the
      # connection, and Thread#raise on a socket ActiveRecord also queries is the
      # most dangerous place for it. Reads are a bare pass-through; the operator
      # bounds them via libpq timeouts in database.yml.
      context "when on the shared-connection (Proc) path" do
        subject(:shared_client) do
          allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
          c = described_class.new(shared_config)
          c.instance_variable_set(:@schema_ensured, true)
          allow(c).to receive(:tune_autovacuum)
          allow(c).to receive(:notify_trigger_current?).and_return(false)
          c
        end

        let(:shared_config) do
          Pgbus::Configuration.new.tap do |c|
            c.connection_params = -> { :raw_conn }
            c.read_timeout = 1
          end
        end

        it "uses the mutex path" do
          expect(shared_client.instance_variable_get(:@pgmq_mutex)).to be_a(Mutex)
        end

        it "never treats the Proc path as libpq-bounded" do
          expect(shared_client.send(:libpq_read_bounds_effective?)).to be(false)
        end

        it "does not wrap reads in Ruby Timeout (bare pass-through)" do
          allow(mock_pgmq).to receive(:read_batch).and_return([])
          allow(Timeout).to receive(:timeout).and_call_original

          shared_client.read_batch("default", qty: 5)

          expect(Timeout).not_to have_received(:timeout)
        end
      end
    end
  end
end
