# frozen_string_literal: true

require "spec_helper"

# Client#resize_streams_pool hot-swap wiring (issue #323 spike). Mirrors
# client_spec.rb's mocked-PGMQ::Client seam: PGMQ::Client.new returns a fresh
# mock per call (job, streams, new_streams), so a swap is observable without a
# DB. Dedicated path via a String database_url.
RSpec.describe Pgbus::Client do
  before do
    allow(described_class).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.streams_pool_size = 5
    end
  end

  let(:job_pgmq)     { build_mock_pgmq }
  let(:streams_pgmq) { build_mock_pgmq }
  let(:new_streams_pgmq) { build_mock_pgmq }

  # Dedicated-path client: PGMQ::Client.new is called for the job pool then the
  # streams pool at construction; a third call happens on resize.
  def build_client
    allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq, new_streams_pgmq)
    c = described_class.new(config, schema_ensured: true)
    allow(c).to receive(:tune_autovacuum)
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    allow(c).to receive(:ensure_stream_queue)
    c
  end

  describe "#resize_streams_pool (dedicated path)" do
    it "builds a new PGMQ::Client at the new size and routes produce to it" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)
      allow(new_streams_pgmq).to receive(:stats).and_return(size: 12, available: 12)

      client.resize_streams_pool(12)
      client.send_stream_message("chat", { "html" => "x" })

      expect(new_streams_pgmq).to have_received(:produce)
      expect(streams_pgmq).not_to have_received(:produce)
    end

    it "closes the old streams pool after the swap, not the new one" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)
      allow(new_streams_pgmq).to receive(:stats).and_return(size: 12, available: 12)

      client.resize_streams_pool(12)

      expect(streams_pgmq).to have_received(:close)
      expect(new_streams_pgmq).not_to have_received(:close)
    end

    it "increments swap telemetry on a real swap" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)
      allow(new_streams_pgmq).to receive(:stats).and_return(size: 12, available: 12)

      expect { client.resize_streams_pool(12) }
        .to change { client.streams_swap_stats.swap_count }.from(0).to(1)
    end

    it "is a no-op (no new client, no close) when the size is unchanged" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)

      result = client.resize_streams_pool(5)

      expect(result).to eq(swapped: false, reason: :unchanged)
      expect(streams_pgmq).not_to have_received(:close)
      expect(client.streams_swap_stats.swap_count).to eq(0)
    end

    it "rejects a non-positive or non-integer size" do
      client = build_client
      [0, -1, "5", 2.5].each do |bad|
        expect { client.resize_streams_pool(bad) }.to raise_error(ArgumentError)
      end
    end

    it "keeps concurrent stats reads safe during a resize (never nil, never raises)" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)
      allow(new_streams_pgmq).to receive(:stats).and_return(size: 12, available: 12)

      errors = []
      reader = Thread.new do
        200.times do
          client.streams_pool_stats
        rescue StandardError => e
          errors << e
        end
      end
      client.resize_streams_pool(12)
      reader.join(2)

      expect(errors).to be_empty
    end
  end

  describe "#resize_streams_pool (shared-AR path)" do
    let(:config) do
      Pgbus::Configuration.new.tap { |c| c.queue_prefix = "pgbus_test" }
    end

    def build_shared_client
      allow(config).to receive(:connection_options).and_return(-> { double("PG::Connection") })
      allow(PGMQ::Client).to receive(:new).and_return(job_pgmq)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    it "is a no-op — the streams pool aliases the job pool" do
      client = build_shared_client

      result = client.resize_streams_pool(12)

      expect(result).to eq(swapped: false, reason: :shared_connection)
      # Only the single job PGMQ::Client was ever built.
      expect(PGMQ::Client).to have_received(:new).once
    end
  end

  describe "#close after a swap" do
    it "closes the current (new) streams pool, and the old was already closed by the swap" do
      client = build_client
      allow(streams_pgmq).to receive(:stats).and_return(size: 5, available: 5)
      allow(new_streams_pgmq).to receive(:stats).and_return(size: 12, available: 12)

      client.resize_streams_pool(12)
      client.close

      expect(streams_pgmq).to have_received(:close).once # by the swap
      expect(new_streams_pgmq).to have_received(:close).once # by client.close
    end
  end
end
