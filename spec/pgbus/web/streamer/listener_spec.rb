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
      logger: logger,
      connection_factory: -> { factory_conns.shift }
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    # reconnect! validates the connection is a primary via pg_is_in_recovery().
    # The fake_pg's #exec returns nil, so neutralize the validator by default
    # (it passes the connection through). The replica-rejection describe re-stubs
    # it to raise ReplicaConnectionError.
    allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) { |conn| conn }
  end

  # A scripted stand-in for PG::Connection. The real PG::Connection blocks on
  # wait_for_notify until a NOTIFY arrives or the timeout fires; we simulate
  # that by pulling from an internal Queue. The listener thread sees real
  # blocking semantics without needing a real Postgres.
  let(:fake_pg) do
    Class.new do
      attr_reader :executed, :reset_count, :close_count

      def initialize
        @executed = []
        @events = Queue.new
        @reset_count = 0
        @close_count = 0
        @closed = false
        @raise_on_next_listen = false
      end

      def exec(sql)
        @executed << sql
        if sql.start_with?("LISTEN") && @raise_on_next_listen
          @raise_on_next_listen = false
          raise PG::Error, "listen failed mid-rebuild"
        end
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

      # Called by Listener#stop and by the factory reconnect loop when it
      # discards the old/partial connection. Pushes a :close event to
      # unblock any thread currently sitting inside wait_for_notify.
      def close
        @closed = true
        @close_count += 1
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

      # Arm the connection so its next LISTEN raises PG::Error once — used to
      # simulate a partially-built reconnect connection.
      def fail_next_listen!
        @raise_on_next_listen = true
      end
    end.new
  end

  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }
  let(:factory_conns)  { [] }

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

  describe "bounded dispatch queue drop (issue #315 item 3)" do
    # handle_notify is exercised directly: the drop decision is pure and does
    # not need the wait_for_notify thread machinery.
    def build_listener(dispatch_queue_limit:)
      described_class.new(
        pg_connection: fake_pg,
        dispatch_queue: dispatch_queue,
        health_check_ms: 50,
        logger: logger,
        connection_factory: -> { factory_conns.shift },
        dispatch_queue_limit: dispatch_queue_limit
      )
    end

    it "enqueues every wake when the limit is 0 (unbounded, the default)" do
      l = build_listener(dispatch_queue_limit: 0)
      10.times { l.send(:handle_notify, "pgmq.q_chat.INSERT") }
      expect(dispatch_queue.size).to eq(10)
    end

    it "drops a durable wake (payload nil) when the queue is at/over the limit" do
      l = build_listener(dispatch_queue_limit: 2)
      dispatch_queue << :filler_one
      dispatch_queue << :filler_two # queue size now 2 == limit

      l.send(:handle_notify, "pgmq.q_chat.INSERT") # durable, no payload

      expect(dispatch_queue.size).to eq(2) # dropped, not enqueued
      expect(l.dropped_wakes).to eq(1)
    end

    it "still enqueues durable wakes while below the limit" do
      l = build_listener(dispatch_queue_limit: 5)
      3.times { l.send(:handle_notify, "pgmq.q_chat.INSERT") }
      expect(dispatch_queue.size).to eq(3)
      expect(l.dropped_wakes).to eq(0)
    end

    it "NEVER drops an ephemeral wake (payload present) even at the limit" do
      l = build_listener(dispatch_queue_limit: 1)
      dispatch_queue << :filler # size 1 == limit

      l.send(:handle_notify, "pgmq.q_chat.INSERT", '{"html":"<x/>"}')

      expect(dispatch_queue.size).to eq(2) # ephemeral got through
      dispatch_queue.pop # discard the filler
      msg = dispatch_queue.pop
      expect(msg).to be_a(described_class::WakeMessage)
      expect(msg.payload).to eq('{"html":"<x/>"}')
      expect(l.dropped_wakes).to eq(0)
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
    # A fake PG connection that can be told to fail the next LISTEN call.
    # Used by the reconnect-preservation test below to prove that a
    # mid-loop failure inside reconnect! does not drop the canonical
    # subscription set.
    let(:raising_pg) do
      Class.new do
        attr_reader :reset_count, :executed

        def initialize
          @executed = []
          @events = Queue.new
          @reset_count = 0
          @raise_on_next_listen = false
        end

        def exec(sql)
          @executed << sql
          if sql.start_with?("LISTEN") && @raise_on_next_listen
            @raise_on_next_listen = false
            raise PG::Error, "boom"
          end
          nil
        end

        def wait_for_notify(_timeout)
          event = @events.pop
          case event[0]
          when :timeout then nil
          when :raise then raise event[1]
          when :close then raise PG::Error, "closed"
          end
        end

        def reset = (@reset_count += 1)
        def close = (@events << [:close])
        def push_timeout = @events << [:timeout]
        def push_error(error) = @events << [:raise, error]
        def fail_next_listen! = (@raise_on_next_listen = true)
      end.new
    end

    before { stub_const("#{described_class}::RECONNECT_BACKOFF_SECONDS", 0.01) }

    it "rebuilds a fresh connection and re-LISTENs every previously-known channel" do
      new_pg = fake_pg.class.new
      factory_conns << new_pg
      listener.start
      listener.ensure_listening("chat")
      listener.ensure_listening("presence")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      new_pg.push_timeout # keep the fresh connection's loop alive

      # LISTEN is re-issued for both channels on the FRESH factory connection.
      wait_until { new_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }
      expect(new_pg.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"), %(LISTEN "pgmq.q_presence.INSERT"))
      # The old connection is discarded, never reset — a fresh connect was used.
      expect(fake_pg.reset_count).to eq(0)
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
    end

    it "preserves the canonical subscription set when a re-LISTEN raises mid-loop" do
      # First factory conn fails its LISTEN mid-rebuild; the loop discards it,
      # backs off, and rebuilds from the second (good) conn.
      partial = fake_pg.class.new.tap(&:fail_next_listen!)
      good = fake_pg.class.new
      raising_conns = [partial, good]
      factory = -> { raising_conns.shift }
      raising_listener = described_class.new(
        pg_connection: raising_pg, dispatch_queue: dispatch_queue,
        health_check_ms: 50, logger: logger, connection_factory: factory
      )
      raising_listener.start
      raising_listener.ensure_listening("chat")
      raising_listener.ensure_listening("presence")
      raising_pg.push_timeout
      wait_until { raising_listener.listening_to.size == 2 }

      raising_pg.push_error(PG::Error.new("network blip"))
      good.push_timeout # keep the good conn's loop alive
      wait_until { good.executed.count { |s| s.start_with?("LISTEN") } >= 2 }

      # Both channels survive a transient mid-reconnect failure — the earlier
      # version cleared @listening_to before the loop and permanently forgot them.
      expect(raising_listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
      expect(raising_pg.reset_count).to eq(0) # old conn discarded, never reset
      raising_listener.stop
    end
  end

  describe "reconnect via connection_factory (NotifyListener loop)" do
    subject(:listener) do
      described_class.new(
        pg_connection: fake_pg,
        dispatch_queue: dispatch_queue,
        health_check_ms: 50,
        logger: logger,
        connection_factory: -> { factory_conns.shift }
      )
    end

    let(:factory_conns) { [] }

    # A connection whose LISTEN always raises — used to prove the reconnect
    # loop spins on failure and only exits when stop() flips @running.
    let(:always_failing_conn) do
      Class.new do
        attr_reader :close_count

        def initialize = (@close_count = 0)

        def exec(sql)
          raise PG::Error, "always fails" if sql.start_with?("LISTEN")

          nil
        end

        def reset = nil
        def close = (@close_count += 1)
      end
    end

    before { stub_const("#{described_class}::RECONNECT_BACKOFF_SECONDS", 0.01) }

    it "rebuilds via the factory and re-LISTENs every channel on the fresh connection" do
      new_pg = fake_pg.class.new
      factory_conns << new_pg

      listener.start
      listener.ensure_listening("chat")
      listener.ensure_listening("presence")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      new_pg.push_timeout

      wait_until do
        new_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2
      end

      expect(new_pg.executed).to include(
        %(LISTEN "pgmq.q_chat.INSERT"),
        %(LISTEN "pgmq.q_presence.INSERT")
      )
      # The original connection must NOT have been reset — a fresh connect
      # was used instead (the whole point of the factory path).
      expect(fake_pg.reset_count).to eq(0)
      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT"
      )
    end

    it "retries with a fresh factory connection until one succeeds" do
      # The first factory connection fails its first LISTEN (partially built);
      # the loop closes it, backs off, and rebuilds from the successor.
      first = fake_pg.class.new
      second = fake_pg.class.new
      first.fail_next_listen! if first.respond_to?(:fail_next_listen!)
      factory_conns.push(first, second)

      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 1 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      second.push_timeout

      wait_until do
        second.executed.count { |s| s == %(LISTEN "pgmq.q_chat.INSERT") } >= 1
      end

      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    end

    it "survives a ConfigurationError from the factory and recovers on a later attempt" do
      # build_raw_pg_connection can raise Pgbus::ConfigurationError (e.g. a
      # pooled AR connection in streams_connection_options). The reconnect loop
      # must catch it, back off, and retry rather than let it kill the thread.
      good = fake_pg.class.new
      calls = 0
      config_error_factory = lambda do
        calls += 1
        raise Pgbus::ConfigurationError, "bad streams_connection_options" if calls == 1

        good
      end
      failing_listener = described_class.new(
        pg_connection: fake_pg, dispatch_queue: dispatch_queue, health_check_ms: 50,
        logger: logger, connection_factory: config_error_factory
      )

      failing_listener.start
      failing_listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { failing_listener.listening_to.size == 1 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      good.push_timeout

      wait_until { good.executed.count { |s| s == %(LISTEN "pgmq.q_chat.INSERT") } >= 1 }
      expect(failing_listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
      failing_listener.stop
    end

    it "closes the partially-built connection before retrying (no leak)" do
      half_built = fake_pg.class.new
      good = fake_pg.class.new
      half_built.fail_next_listen!
      factory_conns.push(half_built, good)

      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 1 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      good.push_timeout

      wait_until { half_built.close_count >= 1 }
      expect(half_built.close_count).to be >= 1
    end

    it "exits the reconnect loop promptly when stop is called mid-retry" do
      # The factory returns a brand-new failing connection on every call, so the
      # reconnect loop never runs dry and would spin forever unless stop() breaks
      # it. We prove stop returns and the thread joins within the timeout.
      infinite_listener = described_class.new(
        pg_connection: fake_pg, dispatch_queue: dispatch_queue, health_check_ms: 50,
        logger: logger, connection_factory: -> { always_failing_conn.new }
      )
      infinite_listener.start
      infinite_listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { infinite_listener.listening_to.size == 1 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      sleep 0.05 # let the loop spin through several failing attempts
      thread = infinite_listener.instance_variable_get(:@thread)
      infinite_listener.stop
      expect(thread.alive?).to be false
    end
  end

  describe "replica rejection on reconnect (failover self-healing)" do
    before { stub_const("#{described_class}::RECONNECT_BACKOFF_SECONDS", 0.01) }

    it "keeps retrying while a fresh conn lands on a replica, then re-LISTENs on the primary" do
      # First factory conn is a replica (validate raises); the next is the
      # promoted primary (validate passes). reconnect! loops internally on
      # validation failure, backing off between cycles.
      replica = fake_pg.class.new
      primary = fake_pg.class.new
      factory_conns.push(replica, primary)
      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 1 }

      calls = 0
      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) do |conn|
        (calls += 1) == 1 ? raise(Pgbus::Process::ReplicaConnectionError, "on a replica") : conn
      end

      fake_pg.push_error(PG::Error.new("connection reset"))
      primary.push_timeout # keep the primary conn's loop alive
      wait_until(timeout: 3) { primary.executed.count { |s| s == %(LISTEN "pgmq.q_chat.INSERT") } >= 1 }

      expect(primary.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"))
      expect(fake_pg.reset_count).to eq(0) # old conn discarded, never reset
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    end

    it "does not re-LISTEN channels while still on a replica" do
      # Every reconnect attempt lands on the same replica; validate_primary!
      # always raises, so the loop spins without ever re-LISTENing.
      replica = fake_pg.class.new
      factory_conns.push(replica)
      allow(factory_conns).to receive(:shift).and_return(replica)
      listener.start
      listener.ensure_listening("chat")
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 1 }

      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!)
        .and_raise(Pgbus::Process::ReplicaConnectionError.new("on a replica"))
      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until { replica.close_count >= 1 } # loop consumed a fresh conn and discarded it
      sleep 0.1

      # A replica reconnect must not re-LISTEN — the channel would register on a
      # host that never NOTIFYs. The subscription set is preserved for the next cycle.
      expect(replica.executed).to be_empty
      expect(fake_pg.reset_count).to eq(0) # old conn discarded, never reset
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    end
  end

  describe "ensure_listening synchronous handshake" do
    it "blocks until the listener thread has actually executed the LISTEN" do
      listener.start

      # Run ensure_listening on a separate thread so we can observe
      # whether it's still waiting on the ack while wait_for_notify
      # is still blocking the listener thread.
      acked = false
      caller_thread = Thread.new do
        listener.ensure_listening("chat")
        acked = true
      end

      # Give the caller thread a chance to push and start blocking on
      # the ack queue. With the old async behavior `acked` would
      # already be true here.
      sleep 0.05
      expect(acked).to be false
      expect(fake_pg.executed.any? { |s| s.include?("LISTEN") }).to be false

      # Unblock the listener thread; it will run drain_commands → do_listen → ack.
      fake_pg.push_timeout
      caller_thread.join(2)

      expect(acked).to be true
      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"))
    end
  end
end
