# frozen_string_literal: true

require "spec_helper"
require "socket"

RSpec.describe Pgbus::Web::Streamer::IoWriter do
  # A lightweight stand-in for Pgbus::Web::Streamer::Connection. IoWriter only
  # needs an #io and a #mutex; building a real Connection here would drag in
  # envelope encoding and couple the IoWriter test to Connection semantics.
  let(:stub_connection_class) do
    Class.new do
      attr_reader :io, :mutex

      def initialize(io)
        @io = io
        @mutex = Mutex.new
      end
    end
  end

  def build_socket_pair
    # UNIXSocket.pair gives us two connected endpoints with blocking semantics
    # identical to a real TCP socket — crucially including WaitWritable when
    # the send buffer fills up, which is the behaviour we're actually testing.
    UNIXSocket.pair
  end

  describe ".write" do
    it "writes all bytes and returns :ok when the reader keeps up" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      # Run the writer synchronously, drain the reader in a thread to keep
      # the pipe empty so we never hit WaitWritable.
      drain_thread = Thread.new { reader_io.read(5) }

      result = described_class.write(conn, "hello", deadline_ms: 1_000)
      expect(result).to eq(:ok)

      writer_io.close
      drain_thread.join
      reader_io.close
    end

    it "returns :closed when the peer has gone away (EPIPE)" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      reader_io.close
      # On Linux+macOS, writing to a half-closed UNIX socket raises EPIPE.
      # SIGPIPE is ignored for Ruby IO writes by default — we get the errno.
      result = described_class.write(conn, "hello", deadline_ms: 1_000)
      expect(result).to eq(:closed)

      writer_io.close
    end

    it "serialises concurrent writers on the same connection via the mutex" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      # Drain the reader so writes can complete without blocking.
      drained = +""
      drain_thread = Thread.new { drained << reader_io.read(8) }

      threads = 2.times.map do |i|
        Thread.new do
          described_class.write(conn, "AAAA"[0, 4], deadline_ms: 1_000) if i.zero?
          described_class.write(conn, "BBBB"[0, 4], deadline_ms: 1_000) unless i.zero?
        end
      end
      threads.each(&:join)

      writer_io.close
      drain_thread.join
      reader_io.close

      # Both writes went through — we can't easily assert byte-interleaving
      # from the reader side because it's one socket, but we can assert the
      # total bytes written and that the mutex did not deadlock.
      expect(drained.length).to eq(8)
      expect(drained.chars.sort).to eq(%w[A A A A B B B B])
    end

    it "returns :blocked when the deadline expires with bytes still unwritten" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      # Fill the OS send buffer by writing without draining the reader.
      # We don't know the exact buffer size, so we push data in chunks until
      # we see WaitWritable. Then we call .write with a short deadline and
      # expect :blocked back.
      large_chunk = "x" * 65_536
      begin
        loop { writer_io.write_nonblock(large_chunk) }
      rescue IO::WaitWritable
        # Expected — the send buffer is now full.
      end

      result = described_class.write(conn, "more data", deadline_ms: 100)
      expect(result).to eq(:blocked)

      writer_io.close
      reader_io.close
    end

    it "handles partial writes by looping until all bytes are sent" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      # Drain slowly — one byte at a time with short sleeps. This forces the
      # IoWriter to loop through multiple write_nonblock calls rather than
      # completing in one shot.
      total_bytes = 2_000
      drained = +""
      drain_thread = Thread.new do
        while drained.length < total_bytes
          begin
            chunk = reader_io.read_nonblock(100)
            drained << chunk
          rescue IO::WaitReadable
            reader_io.wait_readable(0.5)
          end
        end
      end

      result = described_class.write(conn, "y" * total_bytes, deadline_ms: 5_000)
      expect(result).to eq(:ok)

      writer_io.close
      drain_thread.join
      reader_io.close
      expect(drained.length).to eq(total_bytes)
    end

    it "returns :closed on Errno::ECONNRESET" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      # Simulate ECONNRESET by stubbing write_nonblock directly — we can't
      # reliably reproduce ECONNRESET across platforms with UNIXSocket.
      allow(writer_io).to receive(:write_nonblock).and_raise(Errno::ECONNRESET)

      result = described_class.write(conn, "hello", deadline_ms: 1_000)
      expect(result).to eq(:closed)

      writer_io.close
      reader_io.close
    end

    it "returns :closed on IOError (closed stream)" do
      writer_io, reader_io = build_socket_pair
      conn = stub_connection_class.new(writer_io)

      writer_io.close
      result = described_class.write(conn, "hello", deadline_ms: 1_000)
      expect(result).to eq(:closed)

      reader_io.close
    end
  end
end
