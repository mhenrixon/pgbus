# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::NotifyListener do
  subject(:listener) do
    described_class.new(
      physical_queues: %w[pgbus_default pgbus_low],
      on_wake: -> { wakes << :woke },
      connection_options: { dbname: "fake" },
      health_check_ms: 50,
      logger: logger
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    allow(listener).to receive(:build_connection).and_return(fake_pg)
    # The self-probe owns its own connection dance and is covered by
    # notify_probe_spec. Neutralize it by default so it doesn't consume events
    # from the fake connection's queue; the probe-specific describe re-stubs it.
    allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)
    # PrimaryValidator runs SELECT pg_is_in_recovery() on every built connection.
    # The default fake_pg's #exec returns nil, so neutralize the validator by
    # default (it passes the connection through). The replica-rejection describe
    # re-stubs it to raise ReplicaConnectionError.
    allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) { |conn| conn }
  end

  let(:fake_pg) do
    Class.new do
      attr_reader :executed, :close_count

      def initialize
        @executed = []
        @events = Queue.new
        @exec_errors = []
        @close_count = 0
      end

      def exec(sql)
        @executed << sql
        raise @exec_errors.shift if @exec_errors.any?

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
        when :raise
          raise event[1]
        when :close
          raise PG::Error, "connection closed"
        end
      end

      def close
        @close_count += 1
        @events << [:close]
      end

      def closed?
        @close_count.positive?
      end

      def push_notify(channel) = @events << [:notify, channel]
      def push_timeout = @events << [:timeout]
      def push_error(error) = @events << [:raise, error]
      def push_exec_error(error) = @exec_errors << error
    end.new
  end

  let(:wakes)  { Queue.new }
  let(:logger) { Logger.new(IO::NULL) }

  after { listener.stop }

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  describe "#start" do
    it "LISTENs on pgmq.q_<queue>.INSERT for every queue" do
      listener.start
      fake_pg.push_timeout
      wait_until do
        fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_default.INSERT")) &&
          fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_low.INSERT"))
      end

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    it "is idempotent" do
      listener.start
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listens = fake_pg.executed.count { |s| s.start_with?("LISTEN") }
      expect(listens).to eq(2)
    end
  end

  describe "NOTIFY handling" do
    it "fires on_wake for any INSERT notification" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_notify("pgmq.q_pgbus_default.INSERT")

      expect(wakes.pop).to eq(:woke)
    end

    it "coalesces into a single wake per notification" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_notify("pgmq.q_pgbus_low.INSERT")
      expect(wakes.pop).to eq(:woke)
      expect(wakes).to be_empty
    end
  end

  describe "health check" do
    it "runs SELECT 1 when wait_for_notify times out" do
      listener.start
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(fake_pg.executed).to include("SELECT 1")
    end

    it "does not fire on_wake on a timeout" do
      listener.start
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(wakes).to be_empty
    end
  end

  describe "runtime queue-set changes" do
    it "adds a queue via add_queue" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.add_queue("pgbus_webhooks")
      fake_pg.push_timeout
      wait_until { listener.listening_to.include?("pgmq.q_pgbus_webhooks.INSERT") }

      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_pgbus_webhooks.INSERT"))
    end

    it "drops a queue via remove_queue" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.remove_queue("pgbus_low")
      fake_pg.push_timeout
      wait_until { !listener.listening_to.include?("pgmq.q_pgbus_low.INSERT") }

      expect(fake_pg.executed).to include(%(UNLISTEN "pgmq.q_pgbus_low.INSERT"))
    end
  end

  describe "reconnect on connection error" do
    it "rebuilds the connection and re-LISTENs every channel" do
      # Don't use Object#clone — it shallow-copies @events/@executed, so a
      # missed reconnect could pass by reading channels from the first conn.
      second_pg = fake_pg.class.new
      call_count = 0
      allow(listener).to receive(:build_connection) do
        call_count += 1
        call_count == 1 ? fake_pg : second_pg
      end

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      second_pg.push_timeout

      wait_until do
        second_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2
      end

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    context "when re-LISTEN fails mid-rebuild (no leaked conn)" do
      let(:half_built) do
        fake_pg.class.new.tap { |c| c.push_exec_error(PG::Error.new("listen failed mid-rebuild")) }
      end
      let(:good_pg) { fake_pg.class.new }

      before do
        call_sequence = [fake_pg, half_built, good_pg]
        allow(listener).to receive(:build_connection) { call_sequence.shift }
        stub_const("Pgbus::Process::NotifyListener::RECONNECT_BACKOFF_SECONDS", 0.01)
        listener.start
        fake_pg.push_timeout
        wait_until { listener.listening_to.size == 2 }

        fake_pg.push_error(PG::Error.new("connection reset"))
        good_pg.push_timeout
        wait_until { half_built.closed? }
        wait_until { good_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }
      end

      it "closes the partially-built conn before retrying" do
        expect(half_built).to be_closed
      end

      it "ends up subscribed via the successor connection" do
        expect(listener.listening_to).to contain_exactly(
          "pgmq.q_pgbus_default.INSERT",
          "pgmq.q_pgbus_low.INSERT"
        )
      end
    end
  end

  describe "replica rejection (failover self-healing)" do
    context "when a reconnect lands on a replica then converges on the primary" do
      let(:replica_pg) { fake_pg.class.new }
      let(:primary_pg) { fake_pg.class.new }

      before do
        # Initial conn (fake_pg) is a primary; the first reconnect attempt lands
        # on a replica; the second reconnect attempt lands on the primary.
        call_sequence = [fake_pg, replica_pg, primary_pg]
        allow(listener).to receive(:build_connection) { call_sequence.shift }
        # validate_primary! passes fake_pg and primary_pg through, but raises
        # for replica_pg — mirroring pg_is_in_recovery() => t on the demoted host.
        allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) do |conn|
          raise Pgbus::Process::ReplicaConnectionError, "on a replica" if conn.equal?(replica_pg)

          conn
        end
        stub_const("Pgbus::Process::NotifyListener::RECONNECT_BACKOFF_SECONDS", 0.01)

        listener.start
        fake_pg.push_timeout
        wait_until { listener.listening_to.size == 2 }

        fake_pg.push_error(PG::Error.new("connection reset"))
        primary_pg.push_timeout
        wait_until { primary_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }
      end

      it "closes the replica connection before retrying" do
        expect(replica_pg).to be_closed
      end

      it "re-LISTENs every channel on the promoted primary" do
        expect(listener.listening_to).to contain_exactly(
          "pgmq.q_pgbus_default.INSERT",
          "pgmq.q_pgbus_low.INSERT"
        )
        expect(primary_pg.executed).to include(
          %(LISTEN "pgmq.q_pgbus_default.INSERT"),
          %(LISTEN "pgmq.q_pgbus_low.INSERT")
        )
      end
    end

    it "validates the initial connection is a primary at start" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(Pgbus::Process::PrimaryValidator).to have_received(:validate_primary!).with(fake_pg)
    end

    it "does not run the delivery probe when the initial connection is a replica" do
      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!)
        .and_raise(Pgbus::Process::ReplicaConnectionError.new("on a replica"))

      listener.start
      # running? flips to false in the run-loop ensure once the thread exits.
      wait_until { !listener.running? }

      # The replica is rejected before the delivery probe runs (probe is only
      # meaningful on a primary), and the thread exits via the fatal/ensure path.
      expect(Pgbus::Process::NotifyProbe).not_to have_received(:probe_notify_delivery!)
      expect(listener.running?).to be(false)
    end
  end

  describe "LISTEN/NOTIFY self-probe at start" do
    it "runs the probe once against the initial connection" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).once
      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).with(fake_pg, logger: logger)
    end

    it "still starts and LISTENs when the probe fails (graceful degradation)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(false)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    it "does not re-probe on reconnect (probe is start-only)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      second_pg = fake_pg.class.new
      call_count = 0
      allow(listener).to receive(:build_connection) do
        call_count += 1
        call_count == 1 ? fake_pg : second_pg
      end

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      second_pg.push_timeout
      wait_until { second_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }

      # Probe ran for the initial connection only, not the reconnect.
      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).once
    end

    it "reports delivering? true once the probe succeeds (issue #332)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.delivering?).to be(true)
    end

    it "reports delivering? false when the probe fails (pooler-deaf, issue #332)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(false)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.delivering?).to be(false)
    end
  end

  describe "#delivering? (public)" do
    it "is true before the probe runs (optimistic default — assume healthy)" do
      # Until the start-time probe has run, treat the listener as delivering so
      # a not-yet-started listener isn't mistaken for a pooler-deaf one.
      expect(listener.delivering?).to be(true)
    end
  end

  describe "#running? (public)" do
    it "is false before start" do
      expect(listener.running?).to be(false)
    end

    it "is true while the listener thread is alive" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.running?).to be(true)
    end

    it "reflects thread death after a fatal run_loop error" do
      # A connection built at start that dies mid-loop drives run_loop through
      # its rescue/ensure path, which clears @running. The worker relies on
      # running? flipping to false to detect a dead listener and restart it.
      allow(listener).to receive(:build_connection)
        .and_raise(PG::Error.new("boot failed"))

      listener.start
      wait_until { !listener.running? }

      expect(listener.running?).to be(false)
    end
  end

  describe "#stop" do
    it "interrupts the blocking wait and clears the LISTEN set" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.stop
      expect(listener.listening_to).to be_empty
    end

    it "clears running state after the thread exits so start can spawn a fresh thread" do
      allow(listener).to receive(:build_connection)
        .and_raise(PG::Error.new("boot failed"))

      listener.start
      # Thread crashes immediately on build_connection; running? flips back to
      # false in the run-loop ensure.
      wait_until { !listener.running? }

      expect(listener.running?).to be(false)
    end
  end
end
