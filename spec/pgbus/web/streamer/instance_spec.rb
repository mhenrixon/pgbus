# frozen_string_literal: true

require "spec_helper"
require "socket"

RSpec.describe Pgbus::Web::Streamer::Instance do
  subject(:streamer) do
    described_class.new(
      client: client,
      config: config,
      pg_connection: fake_pg,
      logger: Logger.new(IO::NULL)
    )
  end

  before do
    # PG::Error is used by Listener and is not loaded in unit tests.
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  # Reuse the same fake PG connection shape from listener_spec so the
  # Listener thread has something to pop from.
  let(:fake_pg) do
    Class.new do
      attr_reader :executed

      def initialize
        @executed = []
        @events = Queue.new
      end

      def exec(sql)
        @executed << sql
        nil
      end

      def wait_for_notify(_timeout)
        event = @events.pop
        case event[0]
        when :notify
          yield event[1], 0, nil
          event[1]
        when :timeout
          nil
        when :close
          raise PG::Error, "closed"
        end
      end

      def reset = nil
      def close = @events << [:close]
      def push_timeout = @events << [:timeout]
    end.new
  end

  let(:client) do
    instance_double(
      Pgbus::Client,
      read_after: [],
      stream_current_msg_id: 0,
      ensure_stream_queue: nil,
      config: config
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.streams_listen_health_check_ms = 50
      c.streams_heartbeat_interval = 0.05
      c.streams_idle_timeout = 60
      c.streams_write_deadline_ms = 1_000
    end
  end

  after { streamer.shutdown! }

  def build_sse_connection(id: "c1", stream: "chat", since_id: 0)
    writer_io, reader_io = UNIXSocket.pair
    conn = Pgbus::Web::Streamer::Connection.new(
      id: id,
      stream_name: stream,
      io: writer_io,
      since_id: since_id,
      writer: Pgbus::Web::Streamer::IoWriter,
      write_deadline_ms: config.streams_write_deadline_ms
    )
    [conn, reader_io]
  end

  describe "#start" do
    it "starts the listener, dispatcher, and heartbeat threads" do
      streamer.start
      fake_pg.push_timeout
      expect(streamer.listener).to be_a(Pgbus::Web::Streamer::Listener)
      expect(streamer.dispatcher).to be_a(Pgbus::Web::Streamer::StreamEventDispatcher)
      expect(streamer.heartbeat).to be_a(Pgbus::Web::Streamer::Heartbeat)
    end

    it "is idempotent — calling start twice does not spawn duplicate threads" do
      streamer.start
      fake_pg.push_timeout
      expect { streamer.start }.not_to raise_error
    end
  end

  describe "#register" do
    it "enqueues a ConnectMessage that the dispatcher processes" do
      streamer.start
      fake_pg.push_timeout

      conn, reader = build_sse_connection
      streamer.register(conn)

      deadline = Time.now + 2
      sleep 0.01 until streamer.registry.connections_for("chat").include?(conn) || Time.now > deadline

      expect(streamer.registry.connections_for("chat")).to include(conn)
      reader.close
      conn.io.close
    end

    it "is a no-op for a connection that is already dead" do
      streamer.start
      fake_pg.push_timeout
      conn, reader = build_sse_connection
      conn.mark_dead!

      streamer.register(conn)
      sleep 0.1

      expect(streamer.registry.connections_for("chat")).not_to include(conn)
      reader.close
      conn.io.close
    end

    it "rejects a registration that races worker shutdown" do
      # Without the @shutdown_mutex guard in register, a request thread
      # arriving after shutdown! has stopped the dispatcher would push
      # a ConnectMessage onto a queue no one is draining, leaving the
      # socket outside the registry and outside close_all_connections.
      # The fix marks such late connections dead so the caller knows
      # registration was rejected.
      streamer.start
      fake_pg.push_timeout
      streamer.shutdown!

      conn, reader = build_sse_connection(id: "late")
      streamer.register(conn)

      expect(conn.dead?).to be true
      expect(streamer.registry.connections_for("chat")).not_to include(conn)
      expect(streamer.dispatch_queue).to be_empty
      reader.close
      conn.io.close
    end
  end

  describe "#shutdown!" do
    def drain_until(reader, needle, timeout: 2)
      buffer = +""
      Thread.new do
        until buffer.include?(needle)
          begin
            buffer << reader.read_nonblock(4096)
          rescue IO::WaitReadable
            reader.wait_readable(0.1)
          rescue EOFError
            break
          end
        end
      end.join(timeout)
      buffer
    end

    it "writes a pgbus:shutdown sentinel to every registered connection" do
      streamer.start
      fake_pg.push_timeout
      conn, reader = build_sse_connection
      streamer.registry.register(conn)

      Thread.new do
        sleep 0.05
        streamer.shutdown!
      end
      drained = drain_until(reader, "pgbus:shutdown")
      reader.close

      expect(drained).to include("event: pgbus:shutdown")
      expect(drained).to include('data: {"reason":"worker_restart"}')
    end

    it "is idempotent — calling shutdown! twice is a no-op the second time" do
      streamer.start
      fake_pg.push_timeout
      streamer.shutdown!
      expect { streamer.shutdown! }.not_to raise_error
    end
  end

  describe "module-level Streamer.current / Streamer.reset!" do
    after { Pgbus::Web::Streamer.reset! }

    it "memoises a single instance across calls" do
      fake_pg2 = fake_pg.class.new
      instance1 = described_class.new(
        client: client, config: config, pg_connection: fake_pg2,
        logger: Logger.new(IO::NULL)
      )
      Pgbus::Web::Streamer.current = instance1
      expect(Pgbus::Web::Streamer.current).to be(instance1)
    end

    it "resets and tears down the current instance" do
      fake_pg2 = fake_pg.class.new
      instance1 = described_class.new(
        client: client, config: config, pg_connection: fake_pg2,
        logger: Logger.new(IO::NULL)
      ).tap(&:start)
      fake_pg2.push_timeout
      Pgbus::Web::Streamer.current = instance1

      Pgbus::Web::Streamer.reset!

      expect(Pgbus::Web::Streamer.instance_variable_get(:@current)).to be_nil
    end

    it "serialises concurrent first-callers so only one Instance is built" do
      # Without @current_mutex, two threads racing into Streamer.current
      # would both build and start an Instance, leaking the loser of
      # the `@current ||=` race.
      build_count = Concurrent::AtomicFixnum.new(0)
      original_new = described_class.method(:new)
      allow(described_class).to receive(:new) do |**kwargs|
        build_count.increment
        sleep 0.01 # widen the race window so the GVL doesn't accidentally serialise
        original_new.call(**kwargs)
      end

      fake_pgs = Array.new(10) { fake_pg.class.new }
      instances = Concurrent::Array.new
      threads = Array.new(10) do |i|
        Thread.new do
          instances << Pgbus::Web::Streamer.current(client: client, config: config, pg_connection: fake_pgs[i], logger: Logger.new(IO::NULL))
        end
      end
      threads.each(&:join)
      fake_pgs.each(&:push_timeout)

      expect(instances.uniq.size).to eq(1)
      expect(build_count.value).to eq(1)
    end
  end
end
