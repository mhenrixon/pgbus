# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe Pgbus::Web::Streamer::Connection do
  # A test writer that records every call and always succeeds. Models the
  # real Pgbus::Web::Streamer::IoWriter interface (arrives in Phase 2.3):
  #   .write(connection, bytes, deadline_ms:) -> :ok | :closed | :blocked
  subject(:conn) do
    described_class.new(
      id: "c1",
      stream_name: stream,
      io: io,
      since_id: 1247,
      writer: writer,
      write_deadline_ms: 5_000
    )
  end

  let(:writer) do
    Class.new do
      attr_reader :writes

      def initialize(result: :ok)
        @writes = []
        @result = result
      end

      def write(connection, bytes, deadline_ms:)
        @writes << { connection: connection, bytes: bytes, deadline_ms: deadline_ms }
        @result
      end
    end.new
  end

  let(:io)      { StringIO.new }
  let(:stream)  { "chat" }

  def build_envelope(msg_id:, payload: "<turbo-stream>X</turbo-stream>")
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: msg_id,
      enqueued_at: "2026-04-07T00:00:00Z",
      payload: payload,
      source: "live"
    )
  end

  describe "attributes" do
    it "exposes id / stream_name / io" do
      expect(conn.id).to eq("c1")
      expect(conn.stream_name).to eq("chat")
      expect(conn.io).to be(io)
    end

    it "initialises last_msg_id_sent to the caller's since_id" do
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "exposes a mutex for per-io serialization" do
      expect(conn.mutex).to be_a(Mutex)
    end
  end

  describe "#enqueue" do
    it "writes envelopes with msg_id strictly greater than last_msg_id_sent" do
      envelopes = [
        build_envelope(msg_id: 1248, payload: "A"),
        build_envelope(msg_id: 1249, payload: "B")
      ]

      written = conn.enqueue(envelopes)

      expect(written).to eq(envelopes)
      expect(writer.writes.length).to eq(2)
      expect(writer.writes.first[:bytes]).to include("id: 1248")
      expect(writer.writes.first[:bytes]).to include("data: A")
      expect(writer.writes.last[:bytes]).to include("id: 1249")
    end

    it "advances last_msg_id_sent to the highest written id" do
      conn.enqueue([build_envelope(msg_id: 1248), build_envelope(msg_id: 1249)])
      expect(conn.last_msg_id_sent).to eq(1249)
    end

    it "skips envelopes with msg_id <= last_msg_id_sent (dedup on overlap)" do
      conn.enqueue([build_envelope(msg_id: 1248)])
      writer.writes.clear

      conn.enqueue([
                     build_envelope(msg_id: 1247, payload: "stale"),
                     build_envelope(msg_id: 1248, payload: "same"),
                     build_envelope(msg_id: 1249, payload: "new")
                   ])

      expect(writer.writes.length).to eq(1)
      expect(writer.writes.first[:bytes]).to include("id: 1249")
      expect(conn.last_msg_id_sent).to eq(1249)
    end

    it "passes the configured write deadline through to the writer" do
      conn.enqueue([build_envelope(msg_id: 1248)])
      expect(writer.writes.first[:deadline_ms]).to eq(5_000)
    end

    it "accepts an empty array as a no-op" do
      expect { conn.enqueue([]) }.not_to raise_error
      expect(writer.writes).to be_empty
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "wraps each envelope in an SSE frame with event: turbo-stream" do
      conn.enqueue([build_envelope(msg_id: 1248, payload: "X")])
      bytes = writer.writes.first[:bytes]
      expect(bytes).to include("id: 1248\n")
      expect(bytes).to include("event: turbo-stream\n")
      expect(bytes).to include("data: X\n")
      expect(bytes).to end_with("\n\n")
    end

    it "stops writing as soon as the writer returns :closed and marks the connection dead" do
      broken = Class.new do
        def write(*, **)
          :closed
        end
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: broken, write_deadline_ms: 5_000
      )

      c.enqueue([build_envelope(msg_id: 1), build_envelope(msg_id: 2)])

      expect(c.dead?).to be true
    end

    it "stops writing as soon as the writer returns :blocked and marks the connection dead" do
      slow = Class.new do
        def write(*, **)
          :blocked
        end
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: slow, write_deadline_ms: 5_000
      )

      c.enqueue([build_envelope(msg_id: 1)])

      expect(c.dead?).to be true
    end

    it "does not advance last_msg_id_sent for envelopes that fail to write" do
      # First call succeeds, second call reports :closed. We want to prove
      # that the cursor advances for the successful write but NOT for the
      # failed one — so mid-batch failure leaves the cursor on the last
      # confirmed write.
      results = %i[ok closed]
      sequencing_writer = Class.new do
        def initialize(results) = @results = results
        def write(*, **)        = @results.shift
      end.new(results)

      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: sequencing_writer, write_deadline_ms: 5_000
      )
      c.enqueue([build_envelope(msg_id: 1), build_envelope(msg_id: 2)])

      expect(c.last_msg_id_sent).to eq(1)
    end
  end

  describe "#write_comment" do
    it "writes an SSE comment via the writer" do
      conn.write_comment("heartbeat 123")
      expect(writer.writes.length).to eq(1)
      expect(writer.writes.first[:bytes]).to eq(": heartbeat 123\n\n")
    end

    it "does not touch last_msg_id_sent" do
      conn.write_comment("ping")
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "marks the connection dead if the writer reports closed" do
      broken = Class.new do
        def write(*, **) = :closed
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: broken, write_deadline_ms: 5_000
      )
      c.write_comment("ping")
      expect(c.dead?).to be true
    end
  end

  describe "#idle_for" do
    it "returns seconds since the last successful write" do
      # Simulate a write at t=0, advance the monotonic clock, check idle_for
      fake_clock = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      conn.enqueue([build_envelope(msg_id: 1248)])

      fake_clock = 42.5
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      expect(conn.idle_for).to be_within(0.01).of(42.5)
    end

    it "counts from construction time when no writes have happened" do
      fake_clock = 100.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      fresh = described_class.new(
        id: "c2", stream_name: "chat", io: io, since_id: 0,
        writer: writer, write_deadline_ms: 5_000
      )
      fake_clock = 105.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      expect(fresh.idle_for).to be_within(0.01).of(5.0)
    end
  end

  describe "#dead?" do
    it "is false on a fresh connection" do
      expect(conn.dead?).to be false
    end

    it "is true after #mark_dead! is called" do
      conn.mark_dead!
      expect(conn.dead?).to be true
    end
  end
end
