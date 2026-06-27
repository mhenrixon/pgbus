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
      context "when read_timeout is set" do
        before { config.read_timeout = 1 }

        it "raises ReadTimeoutError when read_batch exceeds the timeout" do
          allow(mock_pgmq).to receive(:read_batch) { sleep 5 }

          expect { client.read_batch("default", qty: 5) }.to raise_error(Pgbus::ReadTimeoutError)
        end

        it "raises ReadTimeoutError when read_multi exceeds the timeout" do
          allow(mock_pgmq).to receive(:read_multi) { sleep 5 }

          expect { client.read_multi(%w[default], qty: 5) }.to raise_error(Pgbus::ReadTimeoutError)
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
    end
  end
end
