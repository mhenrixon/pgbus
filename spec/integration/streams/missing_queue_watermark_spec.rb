# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #101 regression test.
#
# On a fresh database, the PGMQ queue table for a given stream
# (pgmq.q_<sanitized>) does not exist until the FIRST broadcast calls
# ensure_stream_queue. The canonical use case for streams —
# `pgbus_stream_from Current.user` in a layout — reads the watermark on
# every page render, so the very first render of a never-broadcasted
# stream previously raised PG::UndefinedTable. "No queue" is semantically
# equivalent to "no messages" and must translate to a 0 watermark / empty
# replay rather than a crash. See lib/pgbus/client/read_after.rb.
RSpec.describe "Streams: missing queue watermark (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:stream_name) { "never_broadcast_#{SecureRandom.hex(4)}" }

  describe "#stream_current_msg_id" do
    it "returns 0 when the PGMQ queue table has never been created" do
      expect(client.stream_current_msg_id(stream_name)).to eq(0)
    end
  end

  describe "#stream_oldest_msg_id" do
    it "returns nil when the PGMQ queue table has never been created" do
      expect(client.stream_oldest_msg_id(stream_name)).to be_nil
    end
  end

  describe "#read_after" do
    it "returns [] when the PGMQ queue table has never been created" do
      expect(client.read_after(stream_name, after_id: 0)).to eq([])
    end
  end

  describe "Pgbus::Streams::Stream#current_msg_id" do
    it "returns 0 when the stream's PGMQ queue has never been created" do
      stream = Pgbus::Streams::Stream.new(stream_name)
      expect(stream.current_msg_id).to eq(0)
    end
  end

  describe "after the first broadcast" do
    it "starts returning real msg_ids once the queue table exists" do
      stream = Pgbus::Streams::Stream.new(stream_name)

      # Before the first broadcast: queue doesn't exist, watermark is 0.
      expect(stream.current_msg_id).to eq(0)

      # First broadcast lazily creates the queue.
      stream.broadcast("<turbo-stream></turbo-stream>")

      # After broadcast: real msg_id from the just-created table.
      expect(stream.current_msg_id).to be >= 1
    end
  end
end
