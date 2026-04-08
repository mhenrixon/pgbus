# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams do
  describe Pgbus::Streams::Stream do
    subject(:stream) { described_class.new("chat", client: client) }

    let(:client) do
      instance_double(
        Pgbus::Client,
        ensure_stream_queue: nil,
        send_message: 1248,
        stream_current_msg_id: 1247,
        read_after: []
      )
    end

    describe "#name" do
      it "returns the logical stream name" do
        expect(stream.name).to eq("chat")
      end
    end

    describe "#broadcast" do
      it "ensures the stream queue exists, then publishes the payload wrapped for PGMQ's JSONB column" do
        stream.broadcast("<turbo-stream>X</turbo-stream>")
        expect(client).to have_received(:ensure_stream_queue).with("chat")
        # The HTML is wrapped as {"html" => ...} because PGMQ stores messages
        # as JSONB and won't accept raw HTML. Dispatcher unwraps before
        # delivering to the SSE client.
        expect(client).to have_received(:send_message).with("chat", { "html" => "<turbo-stream>X</turbo-stream>" })
      end

      it "returns the assigned msg_id" do
        expect(stream.broadcast("X")).to eq(1248)
      end

      it "only ensures the queue once across multiple broadcasts on the same stream object" do
        stream.broadcast("A")
        stream.broadcast("B")
        stream.broadcast("C")
        expect(client).to have_received(:ensure_stream_queue).once
      end
    end

    describe "#current_msg_id" do
      it "delegates to the client and forwards the stream name" do
        expect(stream.current_msg_id).to eq(1247)
        expect(client).to have_received(:stream_current_msg_id).with("chat")
      end
    end

    describe "#read_after" do
      it "delegates to the client" do
        stream.read_after(after_id: 100, limit: 50)
        expect(client).to have_received(:read_after).with("chat", after_id: 100, limit: 50)
      end
    end

    describe "with an object that responds to to_gid_param" do
      let(:account) { double("Account", to_gid_param: "gid://app/Account/42") }

      it "derives a stream name from the GID-param" do
        s = described_class.new(account, client: client)
        expect(s.name).to eq("gid://app/Account/42")
      end
    end

    describe "with an array of streamables" do
      let(:account) { double("Account", to_gid_param: "gid://app/Account/42") }

      it "joins them with colons (turbo-rails compatible)" do
        s = described_class.new([account, :messages], client: client)
        expect(s.name).to eq("gid://app/Account/42:messages")
      end
    end
  end

  describe "Pgbus.stream" do
    it "returns a Pgbus::Streams::Stream" do
      expect(Pgbus.stream("chat")).to be_a(Pgbus::Streams::Stream)
    end

    it "wraps the same name on subsequent calls" do
      expect(Pgbus.stream("chat").name).to eq("chat")
    end
  end
end
