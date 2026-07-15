# frozen_string_literal: true

require "spec_helper"

# Public Pgbus::Client#reload (issue #354): operator escape hatch that drops
# every pooled PGMQ connection — job pool AND the live streams pool — and lets
# the pools rebuild lazily on next checkout (pgmq-ruby >= 0.7.1). Recovers
# connections libpq still reports CONNECTION_OK but that are in fact wedged.
RSpec.describe Pgbus::Client do
  describe "#reload" do
    before do
      # Stub the class method that loads pgmq so the faked PGMQ::Client stands;
      # a clean per-example stub, unlike stubbing global Kernel#require.
      allow(described_class).to receive(:load_pgmq_gem!)
      stub_const("PGMQ::Client", Class.new { def initialize(*args, **kwargs); end })
      real_pgmq_connection_error
      stub_pg_library_version
    end

    context "when on the dedicated-connection path" do
      subject(:client) do
        # Client#initialize builds the job pool first, the streams pool second.
        allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
        c = described_class.new(config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        c
      end

      let(:job_pgmq) { build_mock_pgmq }
      let(:streams_pgmq) { build_mock_pgmq }
      let(:config) do
        Pgbus::Configuration.new.tap do |c|
          c.database_url = "postgres://localhost/pgbus_test"
        end
      end

      it "reloads the job pool and the live streams pool and returns true" do
        expect(client.reload).to be(true)
        expect(job_pgmq).to have_received(:reload).once
        expect(streams_pgmq).to have_received(:reload).once
      end

      it "reloads the NEW streams client after a hot-swap, not the retired one" do
        client # construct before re-stubbing PGMQ::Client.new for the swap
        new_streams = build_mock_pgmq
        allow(PGMQ::Client).to receive(:new).and_return(new_streams)
        client.resize_streams_pool(7)

        client.reload

        expect(new_streams).to have_received(:reload).once
        expect(streams_pgmq).not_to have_received(:reload)
      end
    end

    # Reloading on the shared-AR Proc path would close ActiveRecord's own raw
    # connection out from under the application — pgbus won't close a socket
    # it doesn't own.
    context "when on the shared-connection (Proc) path" do
      subject(:shared_client) do
        allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
        c = described_class.new(shared_config, schema_ensured: true)
        allow(c).to receive(:tune_autovacuum)
        allow(c).to receive(:notify_trigger_current?).and_return(false)
        c
      end

      let(:mock_pgmq) { build_mock_pgmq }
      let(:shared_config) do
        Pgbus::Configuration.new.tap do |c|
          c.connection_params = -> { :raw_conn }
        end
      end

      it "no-ops with a warning and returns false" do
        warnings = []
        allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }

        expect(shared_client.reload).to be(false)
        expect(mock_pgmq).not_to have_received(:reload)
        expect(warnings.join).to include("reload skipped")
      end
    end
  end
end
