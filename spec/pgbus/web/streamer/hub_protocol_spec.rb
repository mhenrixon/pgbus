# frozen_string_literal: true

require "spec_helper"
require "socket"

RSpec.describe Pgbus::Web::Streamer::HubProtocol do
  let(:sockets) { UNIXSocket.pair }
  let(:reader) { sockets[0] }
  let(:writer) { sockets[1] }

  after { sockets.each { |s| s.close unless s.closed? } }

  describe ".encode / .read_frame round trip" do
    it "round-trips a message hash" do
      writer.write(described_class.encode({ "t" => "sub", "q" => "pgbus_stream_chat" }))

      expect(described_class.read_frame(reader)).to eq({ "t" => "sub", "q" => "pgbus_stream_chat" })
    end

    it "keeps multiple back-to-back frames separate" do
      writer.write(described_class.encode({ "t" => "ack", "q" => "a" }))
      writer.write(described_class.encode({ "t" => "wake", "q" => "b", "p" => "<div>hi</div>" }))

      expect(described_class.read_frame(reader)).to eq({ "t" => "ack", "q" => "a" })
      expect(described_class.read_frame(reader)).to eq({ "t" => "wake", "q" => "b", "p" => "<div>hi</div>" })
    end

    it "round-trips multibyte payloads (ephemeral HTML is arbitrary UTF-8)" do
      payload = { "t" => "wake", "q" => "chat", "p" => "<div>héllo — ünïcode 🎉</div>" }
      writer.write(described_class.encode(payload))

      expect(described_class.read_frame(reader)).to eq(payload)
    end

    it "reassembles a frame delivered in partial writes" do
      frame = described_class.encode({ "t" => "wake", "q" => "chat", "p" => "x" * 512 })
      t = Thread.new do
        frame.each_char.each_slice(7) do |chunk|
          writer.write(chunk.join)
          sleep 0.001
        end
      end

      expect(described_class.read_frame(reader)).to include("t" => "wake", "q" => "chat")
      t.join
    end
  end

  describe "EOF handling" do
    it "returns nil on a cleanly closed peer" do
      writer.close

      expect(described_class.read_frame(reader)).to be_nil
    end

    it "returns nil on EOF mid-frame (peer died mid-write)" do
      frame = described_class.encode({ "t" => "wake", "q" => "chat", "p" => "x" * 100 })
      writer.write(frame[0, 10])
      writer.close

      expect(described_class.read_frame(reader)).to be_nil
    end

    it "reports a connection reset as EOF (abrupt peer close, Ruby-4.0-visible)" do
      resetting_io = Class.new do
        def read(_count) = raise Errno::ECONNRESET
      end.new

      expect(described_class.read_frame(resetting_io)).to be_nil
    end
  end

  describe "guards" do
    it "rejects an oversized frame announcement without reading it" do
      writer.write([described_class::MAX_FRAME_BYTES + 1].pack("N"))

      expect { described_class.read_frame(reader) }
        .to raise_error(described_class::ProtocolError, /frame too large/i)
    end

    it "rejects an unencodable oversize payload at encode time" do
      huge = { "t" => "wake", "p" => "x" * (described_class::MAX_FRAME_BYTES + 1) }

      expect { described_class.encode(huge) }
        .to raise_error(described_class::ProtocolError, /frame too large/i)
    end

    it "wraps malformed JSON in a ProtocolError" do
      garbage = "not json".b
      writer.write([garbage.bytesize].pack("N") + garbage)

      expect { described_class.read_frame(reader) }
        .to raise_error(described_class::ProtocolError, /malformed/i)
    end

    it "rejects a valid-JSON non-object frame (scalars/arrays would break dispatch)" do
      body = "[1,2,3]".b
      writer.write([body.bytesize].pack("N") + body)

      expect { described_class.read_frame(reader) }
        .to raise_error(described_class::ProtocolError, /expected a JSON object/i)
    end

    it "rejects invalid UTF-8 bytes" do
      body = "\xff\xfe{}".b
      writer.write([body.bytesize].pack("N") + body)

      expect { described_class.read_frame(reader) }
        .to raise_error(described_class::ProtocolError, /invalid UTF-8/i)
    end
  end
end
