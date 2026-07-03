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
    end

    after do
      config.read_timeout = 30
    end

    describe "#with_read_timeout" do
      # The Ruby Timeout is only an outer fallback now (the primary bound is a
      # server-side statement_timeout), firing at read_timeout + slack (~5s).
      # Sleeps here must exceed that combined bound to exercise the fallback.
      context "when read_timeout is set" do
        before { config.read_timeout = 1 }

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

      context "when read_timeout is nil" do
        before { config.read_timeout = nil }

        it "does not wrap reads in a timeout" do
          allow(mock_pgmq).to receive(:read_batch).and_return([])

          expect(client.read_batch("default", qty: 5)).to eq([])
        end
      end

      # On the shared-connection path a single mutex serializes all reads.
      # The timeout clock must start only after the mutex is acquired, so a
      # thread queued behind a slow read is not charged for the wait. Here a
      # fast read whose mutex wait exceeds the timeout must still succeed.
      context "when a read waits on the client mutex (shared connection)" do
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

        it "does not raise ReadTimeoutError for time spent waiting on the mutex" do
          mutex = shared_client.instance_variable_get(:@pgmq_mutex)
          allow(mock_pgmq).to receive(:read_batch).and_return([])

          # Hold the mutex longer than the timeout, then release. The read's
          # own work is instant, so it must complete without a timeout error.
          # Handshake via a Queue so we know the holder is inside the
          # synchronized block before issuing the read — a plain sleep is
          # racy on slow CI runners.
          locked = Queue.new
          holder = Thread.new do
            mutex.synchronize do
              locked << true
              sleep 1.5
            end
          end

          locked.pop
          expect { shared_client.read_batch("default", qty: 5) }.not_to raise_error
        ensure
          holder&.join
        end
      end
    end
  end
end
