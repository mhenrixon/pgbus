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
    allow_any_instance_of(described_class).to receive(:build_connection).and_return(fake_pg)
  end

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
        when :raise
          raise event[1]
        when :close
          raise PG::Error, "connection closed"
        end
      end

      def close = (@events << [:close])
      def push_notify(channel) = @events << [:notify, channel]
      def push_timeout = @events << [:timeout]
      def push_error(error) = @events << [:raise, error]
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
      second_pg = fake_pg.clone
      call_count = 0
      allow_any_instance_of(described_class).to receive(:build_connection) do
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
  end

  describe "#stop" do
    it "interrupts the blocking wait and clears the LISTEN set" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.stop
      expect(listener.listening_to).to be_empty
    end
  end
end
