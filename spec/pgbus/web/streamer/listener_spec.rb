# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::Listener do
  # PG::Error isn't loaded in unit tests (the spec suite stubs pgmq-ruby away).
  # Define a minimal stand-in so Listener's rescue clause has something real
  # to catch. The fake_pg below raises this class when a [:raise] event fires
  # or when #close is called mid-wait.
  subject(:listener) do
    described_class.new(
      pg_connection: fake_pg,
      dispatch_queue: dispatch_queue,
      health_check_ms: 50,
      logger: logger
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  # A scripted stand-in for PG::Connection. The real PG::Connection blocks on
  # wait_for_notify until a NOTIFY arrives or the timeout fires; we simulate
  # that by pulling from an internal Queue. The listener thread sees real
  # blocking semantics without needing a real Postgres.
  let(:fake_pg) do
    Class.new do
      attr_reader :executed, :reset_count

      def initialize
        @executed = []
        @events = Queue.new
        @reset_count = 0
        @closed = false
      end

      def exec(sql)
        @executed << sql
        nil
      end

      # Mirrors PG::Connection#wait_for_notify(timeout) { |channel, pid, payload| ... }
      #   - yields on notify and returns the channel
      #   - returns nil on timeout
      #   - raises on scripted error
      def wait_for_notify(_timeout)
        event = @events.pop
        case event[0]
        when :notify
          yield event[1], 0, nil
          event[1]
        when :timeout
          nil
        when :raise
          raise event[1]
        when :close
          raise PG::Error, "connection closed"
        end
      end

      def reset
        @reset_count += 1
      end

      # Called by Listener#stop. Pushes a :close event to unblock any
      # thread currently sitting inside wait_for_notify.
      def close
        @closed = true
        @events << [:close]
      end

      def push_notify(channel)
        @events << [:notify, channel]
      end

      def push_timeout
        @events << [:timeout]
      end

      def push_error(error)
        @events << [:raise, error]
      end
    end.new
  end

  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }

  after { listener.stop }

  def wait_until(timeout: 2, &block)
    deadline = Time.now + timeout
    until block.call
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  describe "#start and #stop" do
    it "spawns a thread on start and joins it on stop" do
      listener.start
      fake_pg.push_timeout # unblock the wait_for_notify so stop can proceed
      listener.stop
      expect(listener.listening_to).to be_empty
    end
  end

  describe "channel naming" do
    it "LISTENs on pgmq.q_<queue>.INSERT for ensure_listening" do
      listener.start
      listener.ensure_listening("pgbus_stream_chat")
      fake_pg.push_timeout

      wait_until { fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_stream_chat.INSERT")) }
      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_pgbus_stream_chat.INSERT"))
    end

    it "UNLISTENs on remove_listening" do
      listener.start
      listener.ensure_listening("pgbus_stream_chat")
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_stream_chat.INSERT")) }

      listener.remove_listening("pgbus_stream_chat")
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?(%(UNLISTEN "pgmq.q_pgbus_stream_chat.INSERT")) }
      expect(fake_pg.executed).to include(%(UNLISTEN "pgmq.q_pgbus_stream_chat.INSERT"))
    end

    it "records the channel in listening_to" do
      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.include?("pgmq.q_chat.INSERT") }
      expect(listener.listening_to).to include("pgmq.q_chat.INSERT")
    end

    it "is idempotent on duplicate ensure_listening" do
      listener.start
      listener.ensure_listening("chat")
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 1 }

      listen_count = fake_pg.executed.count { |sql| sql.start_with?("LISTEN") }
      expect(listen_count).to eq(1)
    end
  end

  describe "NOTIFY handling" do
    it "posts a WakeMessage with the queue name extracted from the channel" do
      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.include?("pgmq.q_chat.INSERT") }

      fake_pg.push_notify("pgmq.q_chat.INSERT")

      msg = dispatch_queue.pop
      expect(msg).to be_a(described_class::WakeMessage)
      expect(msg.queue_name).to eq("chat")
    end

    it "ignores notifications on channels that don't match the pgmq pattern" do
      listener.start
      fake_pg.push_notify("random_channel")
      fake_pg.push_timeout

      sleep 0.1
      expect(dispatch_queue).to be_empty
    end
  end

  describe "health check" do
    it "runs SELECT 1 when wait_for_notify times out" do
      listener.start
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(fake_pg.executed).to include("SELECT 1")
    end
  end

  describe "reconnect on PG::Error" do
    it "calls reset and re-LISTENs on every previously-known channel" do
      listener.start

      listener.ensure_listening("chat")
      listener.ensure_listening("presence")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      pg_error = PG::Error.new("connection reset")
      fake_pg.push_error(pg_error)

      wait_until { fake_pg.reset_count >= 1 }

      # Keep the loop alive so we can observe post-reconnect state
      fake_pg.push_timeout

      # After reconnect we expect LISTEN to be issued a second time for both
      # channels. The `executed` array accumulates across the whole run.
      wait_until do
        chat = fake_pg.executed.count { |s| s == %(LISTEN "pgmq.q_chat.INSERT") }
        presence = fake_pg.executed.count { |s| s == %(LISTEN "pgmq.q_presence.INSERT") }
        chat >= 2 && presence >= 2
      end

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_chat.INSERT",
        "pgmq.q_presence.INSERT"
      )
    end
  end
end
