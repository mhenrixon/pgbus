# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #310 regression test.
#
# When config.priority_levels > 1, Client#send_message routes every send
# through the priority strategy to a _p<default_priority> sub-queue. Durable
# stream broadcasts must NOT be routed that way: the streamer LISTENs and
# replays on the BARE queue (pgmq.q_<stream>), so a broadcast that landed in
# _p1 would never reach the browser — and on a fresh DB, ensure_stream_queue
# would raise because it enables NOTIFY on the (uncreated) bare table.
#
# Stream broadcasts go through Client#send_stream_message, which always
# targets the bare queue and creates it via ensure_single_queue. This test
# proves that with priority ON, a durable broadcast is created on and
# replayable from the bare queue.
RSpec.describe "Streams: durable broadcast under priority_levels (integration)", :integration do
  let(:stream_name) { "prio_#{SecureRandom.hex(4)}" }

  around do |example|
    saved = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client! # rebuild @queue_strategy as PriorityStrategy
    example.run
    Pgbus.configuration.priority_levels = saved
    Pgbus.reset_client!
  end

  it "does not raise on the first broadcast (fresh queue) with priority enabled" do
    stream = Pgbus::Streams::Stream.new(stream_name)

    expect { stream.broadcast("<turbo-stream>hello</turbo-stream>") }.not_to raise_error
  end

  it "makes the broadcast replayable from the bare queue, not a _pN sub-queue" do
    stream = Pgbus::Streams::Stream.new(stream_name)

    stream.broadcast("<turbo-stream>one</turbo-stream>")
    stream.broadcast("<turbo-stream>two</turbo-stream>")

    # read_after peeks the bare pgmq.q_<stream> (+ archive) — the exact path
    # the streamer uses. If the broadcast had been misrouted to _p1, this
    # would return []. Both frames must be here, in order.
    envelopes = Pgbus.client.read_after(stream_name, after_id: 0)

    payloads = envelopes.map { |e| JSON.parse(e.payload)["html"] }
    expect(payloads).to eq([
                             "<turbo-stream>one</turbo-stream>",
                             "<turbo-stream>two</turbo-stream>"
                           ])
  end

  it "advances the bare-queue watermark (proves the bare table received the row)" do
    stream = Pgbus::Streams::Stream.new(stream_name)

    expect(stream.current_msg_id).to eq(0)
    stream.broadcast("<turbo-stream>x</turbo-stream>")
    expect(stream.current_msg_id).to be >= 1
  end
end
