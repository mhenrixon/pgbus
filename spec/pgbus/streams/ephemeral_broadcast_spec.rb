# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Stream do
  let(:client) do
    instance_double(
      Pgbus::Client,
      ensure_stream_queue: nil,
      send_message: 1248,
      stream_current_msg_id: 1247,
      read_after: [],
      notify_stream: nil,
      config: config
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.queue_prefix = "pgbus_test"
    end
  end

  describe "durable: false (default for Turbo patch)" do
    subject(:stream) { described_class.new("chat", client: client, durable: false) }

    it "does NOT create a PGMQ queue on broadcast" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).not_to have_received(:ensure_stream_queue)
    end

    it "does NOT send a PGMQ message on broadcast" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).not_to have_received(:send_message)
    end

    it "sends a PG NOTIFY with the payload" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).to have_received(:notify_stream).with(
        "chat",
        { "html" => "<turbo-stream>X</turbo-stream>" }
      )
    end

    it "includes visible_to in the NOTIFY payload when specified" do
      stream.broadcast("<turbo-stream>X</turbo-stream>", visible_to: :admin_only)
      expect(client).to have_received(:notify_stream).with(
        "chat",
        { "html" => "<turbo-stream>X</turbo-stream>", "visible_to" => "admin_only" }
      )
    end

    it "returns nil (no msg_id for ephemeral broadcasts)" do
      expect(stream.broadcast("X")).to be_nil
    end

    it "reports durable? as false" do
      expect(stream).not_to be_durable
    end
  end

  describe "durable: true (explicit opt-in)" do
    subject(:stream) { described_class.new("chat", client: client, durable: true) }

    it "creates the PGMQ queue on first broadcast" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).to have_received(:ensure_stream_queue).with("chat")
    end

    it "sends the message via PGMQ" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).to have_received(:send_message).with("chat", { "html" => "<turbo-stream>X</turbo-stream>" })
    end

    it "does NOT use notify_stream" do
      stream.broadcast("<turbo-stream>X</turbo-stream>")
      expect(client).not_to have_received(:notify_stream)
    end

    it "reports durable? as true" do
      expect(stream).to be_durable
    end

    it "returns the msg_id" do
      expect(stream.broadcast("X")).to eq(1248)
    end
  end

  describe "default durable mode" do
    it "defaults to durable when no mode specified" do
      stream = described_class.new("chat", client: client)
      expect(stream).to be_durable
    end
  end
end
