# frozen_string_literal: true

require "spec_helper"

# Issue #391: an ephemeral broadcast whose JSON exceeds the PG NOTIFY
# payload cap must not raise a misleading connection error (sync path) or
# vanish silently (coalescer flush thread). It auto-degrades to a durable
# publish: payload in PGMQ, the queue's insert trigger fires the NOTIFY as
# a bare wake on the same channel the subscriber already LISTENs on.
RSpec.describe Pgbus::Streams::Stream do
  subject(:stream) { described_class.new("probe", client: client, durable: false) }

  let(:client) do
    instance_double(
      Pgbus::Client,
      ensure_stream_queue: nil,
      send_stream_message: 1248,
      notify_stream: nil,
      config: config
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.queue_prefix = "pgbus_test"
    end
  end

  let(:limit) { Pgbus::Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES }
  let(:big_html) { "<div>#{"x" * 9000}</div>" }

  describe "oversized ephemeral broadcast" do
    it "falls back to a durable publish" do
      stream.broadcast(big_html)
      expect(client).to have_received(:send_stream_message).with("probe", { "html" => big_html })
    end

    it "ensures the stream queue before publishing" do
      stream.broadcast(big_html)
      expect(client).to have_received(:ensure_stream_queue).with("probe")
    end

    it "does not attempt the NOTIFY" do
      stream.broadcast(big_html)
      expect(client).not_to have_received(:notify_stream)
    end

    it "returns the durable msg_id" do
      expect(stream.broadcast(big_html)).to eq(1248)
    end

    it "warn-logs the fallback with stream and byte count" do
      allow(Pgbus.logger).to receive(:warn)
      stream.broadcast(big_html)
      expect(Pgbus.logger).to have_received(:warn)
    end

    it "carries visible_to through to the durable publish" do
      stream.broadcast(big_html, visible_to: :admins)
      expect(client).to have_received(:send_stream_message).with(
        "probe", { "html" => big_html, "visible_to" => "admins" }
      )
    end

    it "instruments the fallback" do
      events = []
      allow(Pgbus::Instrumentation).to receive(:instrument) do |name, payload, &block|
        events << [name, payload]
        block&.call
      end

      stream.broadcast(big_html)

      event = events.find { |(name, _)| name == "pgbus.stream.broadcast" }
      expect(event).not_to be_nil
      expect(event.last).to include(stream: "probe", ephemeral_fallback: true)
    end
  end

  describe "budget boundary" do
    it "measures the wrapped JSON, so metadata pushes a near-cap frame over" do
      # HTML sized so {"html":"..."} is exactly at the limit; adding
      # visible_to overflows the JSON and must trigger the fallback.
      at_limit_html = "x" * (limit - 11)
      stream.broadcast(at_limit_html, visible_to: :admins)
      expect(client).to have_received(:send_stream_message)
      expect(client).not_to have_received(:notify_stream)
    end

    it "keeps a frame exactly at the cap on the NOTIFY path" do
      at_limit_html = "x" * (limit - 11)
      stream.broadcast(at_limit_html)
      expect(client).to have_received(:notify_stream)
      expect(client).not_to have_received(:send_stream_message)
    end
  end

  describe "small ephemeral broadcast" do
    it "stays on the NOTIFY path, passing the pre-serialized JSON" do
      stream.broadcast("<div>hi</div>")
      expect(client).to have_received(:notify_stream).with(
        "probe", JSON.generate({ "html" => "<div>hi</div>" })
      )
      expect(client).not_to have_received(:send_stream_message)
      expect(client).not_to have_received(:ensure_stream_queue)
    end
  end
end
