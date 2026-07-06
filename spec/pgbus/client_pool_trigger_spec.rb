# frozen_string_literal: true

require "spec_helper"

# Client wires a publisher-side autoscale trigger into send_stream_message
# (issue #323 follow-up): only when streams_pool_autoscale is on AND the pool is
# dedicated, and it fires after the produce.
RSpec.describe Pgbus::Client do
  before do
    allow(described_class).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new do
      def initialize(*args, **kwargs); end
    end)
  end

  let(:job_pgmq)     { build_mock_pgmq }
  let(:streams_pgmq) { build_mock_pgmq }

  def build_client(config)
    allow(PGMQ::Client).to receive(:new).and_return(job_pgmq, streams_pgmq)
    c = described_class.new(config, schema_ensured: true)
    allow(c).to receive(:tune_autovacuum)
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    allow(c).to receive(:ensure_stream_queue)
    c
  end

  context "when autoscale is off (default)" do
    let(:config) do
      Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
      end
    end

    it "does not build a trigger and does not check on publish" do
      client = build_client(config)
      allow(Pgbus::Streams::PoolTrigger).to receive(:new).and_call_original

      client.send_stream_message("chat", { "html" => "x" })

      expect(client.send(:streams_pool_trigger)).to be_nil
      expect(Pgbus::Streams::PoolTrigger).not_to have_received(:new)
    end
  end

  context "when autoscale is on and the pool is dedicated" do
    let(:config) do
      Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
        c.streams_pool_autoscale = true
      end
    end

    it "builds a trigger and fires maybe_check after the produce" do
      client = build_client(config)
      trigger = client.send(:streams_pool_trigger)
      expect(trigger).to be_a(Pgbus::Streams::PoolTrigger)

      allow(trigger).to receive(:maybe_check)
      client.send_stream_message("chat", { "html" => "x" })

      expect(trigger).to have_received(:maybe_check)
      expect(streams_pgmq).to have_received(:produce) # produce still happened
    end

    it "returns the produced msg_id, not the trigger result (regression guard)" do
      client = build_client(config)
      trigger = client.send(:streams_pool_trigger)
      allow(trigger).to receive(:maybe_check).and_return(nil) # trigger returns nil
      allow(streams_pgmq).to receive(:produce).and_return(4242)

      result = client.send_stream_message("chat", { "html" => "x" })

      expect(result).to eq(4242) # the msg_id, NOT the trigger's nil
    end

    it "builds exactly one trigger under concurrent first-publishers (no memo race)" do
      client = build_client(config)
      triggers = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(8)

      threads = Array.new(8) do
        Thread.new do
          barrier.wait # release all threads at once to maximize the race
          triggers << client.send(:streams_pool_trigger)
        end
      end
      threads.each(&:join)

      expect(triggers.uniq.size).to eq(1) # every thread got the SAME trigger
    end

    it "memoizes the trigger across publishes" do
      client = build_client(config)
      first = client.send(:streams_pool_trigger)
      second = client.send(:streams_pool_trigger)
      expect(first).to equal(second)
    end
  end

  context "when autoscale is on but the connection is shared (AR path)" do
    let(:config) do
      cfg = Pgbus::Configuration.new.tap do |c|
        c.queue_prefix = "pgbus_test"
        c.streams_pool_autoscale = true
      end
      allow(cfg).to receive(:connection_options).and_return(-> { double("PG::Connection") })
      cfg
    end

    it "does not build a trigger (resize is a no-op on the shared path)" do
      allow(PGMQ::Client).to receive(:new).and_return(job_pgmq)
      c = described_class.new(config, schema_ensured: true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)

      expect(c.send(:streams_pool_trigger)).to be_nil
    end
  end
end
