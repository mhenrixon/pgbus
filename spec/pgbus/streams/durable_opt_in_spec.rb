# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Stream do
  let(:client) do
    instance_double(
      Pgbus::Client,
      ensure_stream_queue: nil,
      send_message: 1248,
      send_stream_message: 1248,
      stream_current_msg_id: 1247,
      read_after: [],
      notify_stream: nil,
      config: Pgbus::Configuration.new
    )
  end

  describe "per-broadcast durable: override" do
    context "when stream is ephemeral by default" do
      subject(:stream) { described_class.new("chat", client: client, durable: false) }

      it "uses durable path when durable: true is passed to broadcast" do
        stream.broadcast("X", durable: true)
        expect(client).to have_received(:ensure_stream_queue).with("chat")
        expect(client).to have_received(:send_stream_message).with("chat", { "html" => "X" })
        expect(client).not_to have_received(:notify_stream)
      end

      it "still uses ephemeral path when no override is passed" do
        stream.broadcast("X")
        expect(client).to have_received(:notify_stream)
        expect(client).not_to have_received(:send_stream_message)
      end
    end

    context "when stream is durable by default" do
      subject(:stream) { described_class.new("chat", client: client, durable: true) }

      it "uses ephemeral path when durable: false is passed to broadcast" do
        stream.broadcast("X", durable: false)
        expect(client).to have_received(:notify_stream)
        expect(client).not_to have_received(:send_stream_message)
        expect(client).not_to have_received(:ensure_stream_queue)
      end

      it "still uses durable path when no override is passed" do
        stream.broadcast("X")
        expect(client).to have_received(:send_stream_message)
      end
    end

    it "passes visible_to alongside the durable override" do
      stream = described_class.new("chat", client: client, durable: false)
      stream.broadcast("X", durable: true, visible_to: :admin)
      expect(client).to have_received(:send_stream_message)
        .with("chat", { "html" => "X", "visible_to" => "admin" })
    end
  end
end
