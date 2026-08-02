# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::NotifyHub do
  subject(:hub) do
    described_class.new(
      config: config,
      listener_factory: listener_factory,
      clock: -> { clock[0] },
      logger: logger
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.queue_prefix = "pgbus_test"
      c.workers = [{ queues: %w[critical], threads: 1 }, { queues: %w[default mailers], threads: 1 }]
      c.database_url = "postgres://fake@localhost/fake"
    end
  end
  let(:logger) { Logger.new(IO::NULL) }
  let(:clock) { [0.0] }

  let(:fake_listener) do
    instance_double(
      Pgbus::Process::NotifyListener,
      stop: nil, running?: true, connected?: true, delivering?: true,
      add_queue: nil, remove_queue: nil, listening_to: []
    ).tap { |l| allow(l).to receive(:start).and_return(l) }
  end
  # Captures the arguments the hub builds its listener with, so specs can
  # assert the union and drive the on_wake routing callback directly.
  let(:factory_calls) { [] }
  let(:listener_factory) do
    lambda do |physical_queues:, on_wake:|
      factory_calls << { physical_queues: physical_queues, on_wake: on_wake }
      fake_listener
    end
  end

  let(:registry) { instance_double(Pgbus::EventBus::Registry, queue_names_for_topics: []) }

  before do
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
    allow(Pgbus::Process::WildcardQueueResolver).to receive(:resolve).and_return([])
  end

  after { hub.stop }

  def make_pipe
    reader, writer = IO.pipe
    [reader, writer]
  end

  def drain(reader)
    reader.read_nonblock(1024)
  rescue IO::WaitReadable
    ""
  end

  describe "#start — LISTEN union" do
    it "listens on the physical channels of every explicit capsule queue" do
      hub.start

      expect(factory_calls.last[:physical_queues])
        .to contain_exactly("pgbus_test_critical", "pgbus_test_default", "pgbus_test_mailers")
    end

    it "includes consumer queues derived from the registry" do
      config.event_consumers = [{ topics: ["orders.#"], threads: 1 }]
      allow(registry).to receive(:queue_names_for_topics).with(["orders.#"]).and_return(%w[orders_handler])

      hub.start

      expect(factory_calls.last[:physical_queues]).to include("pgbus_test_orders_handler")
    end

    it "resolves wildcard capsules through the shared resolver" do
      config.workers = [{ queues: %w[*], threads: 1 }]
      allow(Pgbus::Process::WildcardQueueResolver).to receive(:resolve)
        .with(config: config).and_return(%w[default reports])

      hub.start

      expect(factory_calls.last[:physical_queues])
        .to contain_exactly("pgbus_test_default", "pgbus_test_reports")
    end

    it "skips consumer queues when the consumers role is disabled" do
      config.event_consumers = [{ topics: ["orders.#"], threads: 1 }]
      config.roles = [:workers]
      allow(registry).to receive(:queue_names_for_topics).and_return(%w[orders_handler])

      hub.start

      expect(factory_calls.last[:physical_queues]).not_to include("pgbus_test_orders_handler")
    end

    it "skips capsule queues when the workers role is disabled" do
      config.roles = [:consumers]

      hub.start

      expect(factory_calls.last[:physical_queues]).to be_empty
    end

    it "de-duplicates queues shared between capsules" do
      config.workers = [{ queues: %w[default], threads: 1 }, { queues: %w[default], threads: 1 }]

      hub.start

      expect(factory_calls.last[:physical_queues]).to eq(%w[pgbus_test_default])
    end
  end

  describe "wake routing" do
    let(:pipe_a) { make_pipe }
    let(:pipe_b) { make_pipe }

    before do
      hub.start
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: pipe_a[1])
      hub.register_fork(pid: 200, queues: %w[pgbus_test_default], pipe: pipe_b[1])
      drain(pipe_a[0]) # discard the registration status byte
      drain(pipe_b[0])
    end

    after do
      (pipe_a + pipe_b).each { |io| io.close unless io.closed? }
    end

    it "writes W only to forks whose queue set contains the notifying queue" do
      factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_critical.INSERT")

      expect(drain(pipe_a[0])).to eq("W")
      expect(drain(pipe_b[0])).to eq("")
    end

    it "wakes wildcard forks for any known queue" do
      wild_reader, wild_writer = make_pipe
      hub.register_fork(pid: 300, queues: [], wildcard: true, pipe: wild_writer)
      drain(wild_reader)

      factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_critical.INSERT")

      expect(drain(wild_reader)).to eq("W")
      wild_reader.close
      wild_writer.close
    end

    it "survives a fork whose read end is gone (EPIPE)" do
      pipe_a[0].close

      expect { factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_critical.INSERT") }
        .not_to raise_error
      # Sibling routing is unaffected.
      factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_default.INSERT")
      expect(drain(pipe_b[0])).to eq("W")
    end

    it "skips (not blocks) a fork whose pipe buffer is full" do
      writer = pipe_a[1]
      begin
        loop { writer.write_nonblock("x" * 4096) }
      rescue IO::WaitWritable
        # buffer full
      end

      expect { factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_critical.INSERT") }
        .not_to raise_error
    end

    it "stops waking a deregistered fork and closes its write end" do
      hub.deregister_fork(100)

      expect(pipe_a[1]).to be_closed
      expect { factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_critical.INSERT") }
        .not_to raise_error
    end
  end

  describe "status broadcast" do
    let(:pipe) { make_pipe }

    before { hub.start }

    after { pipe.each { |io| io.close unless io.closed? } }

    it "sends the current status byte on registration" do
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: pipe[1])

      expect(drain(pipe[0])).to eq(Pgbus::Process::WakePipe::HEALTHY)
    end

    it "broadcasts P when the listener loses its connection, and H when it returns" do
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: pipe[1])
      drain(pipe[0])

      allow(fake_listener).to receive(:connected?).and_return(false)
      hub.tick
      expect(drain(pipe[0])).to eq(Pgbus::Process::WakePipe::DEGRADED)

      allow(fake_listener).to receive(:connected?).and_return(true)
      hub.tick
      expect(drain(pipe[0])).to eq(Pgbus::Process::WakePipe::HEALTHY)
    end

    it "treats a live-but-deaf listener (delivering? false) as degraded" do
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: pipe[1])
      drain(pipe[0])

      allow(fake_listener).to receive(:delivering?).and_return(false)
      hub.tick

      expect(drain(pipe[0])).to eq(Pgbus::Process::WakePipe::DEGRADED)
    end

    it "rebroadcasts the unchanged status after the rebroadcast interval" do
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: pipe[1])
      drain(pipe[0])

      hub.tick
      expect(drain(pipe[0])).to eq("") # no change, no rebroadcast due

      clock[0] += described_class::STATUS_REBROADCAST_SECONDS + 1
      hub.tick
      expect(drain(pipe[0])).to eq(Pgbus::Process::WakePipe::HEALTHY)
    end
  end

  describe "listener self-healing" do
    it "restarts a dead listener on tick after the retry backoff" do
      hub.start
      expect(factory_calls.size).to eq(1)

      allow(fake_listener).to receive(:running?).and_return(false)
      clock[0] += described_class::RETRY_BASE_SECONDS + 1
      hub.tick

      expect(factory_calls.size).to eq(2)
      expect(fake_listener).to have_received(:stop).at_least(:once)
    end

    it "does not thrash restarts before the backoff elapses" do
      hub.start
      allow(fake_listener).to receive(:running?).and_return(false)

      clock[0] += described_class::RETRY_BASE_SECONDS + 1
      hub.tick
      hub.tick # immediately after — still inside the doubled backoff

      expect(factory_calls.size).to eq(2)
    end
  end

  describe "union refresh" do
    it "diffs the union through add_queue/remove_queue after the refresh interval" do
      config.workers = [{ queues: %w[*], threads: 1 }]
      allow(Pgbus::Process::WildcardQueueResolver).to receive(:resolve).and_return(%w[default])
      hub.start

      allow(fake_listener).to receive(:listening_to)
        .and_return(["pgmq.q_pgbus_test_default.INSERT"])
      allow(Pgbus::Process::WildcardQueueResolver).to receive(:resolve).and_return(%w[reports])

      clock[0] += described_class::REFRESH_INTERVAL_SECONDS + 1
      hub.tick

      expect(fake_listener).to have_received(:add_queue).with("pgbus_test_reports")
      expect(fake_listener).to have_received(:remove_queue).with("pgbus_test_default")
    end

    it "does not refresh before the interval" do
      config.workers = [{ queues: %w[*], threads: 1 }]
      hub.start

      hub.tick

      expect(fake_listener).not_to have_received(:add_queue)
      expect(fake_listener).not_to have_received(:remove_queue)
    end
  end

  describe "#stop" do
    it "stops the listener and closes every registered write end" do
      reader, writer = make_pipe
      hub.start
      hub.register_fork(pid: 100, queues: %w[pgbus_test_critical], pipe: writer)

      hub.stop

      expect(fake_listener).to have_received(:stop)
      expect(writer).to be_closed
      reader.close
    end
  end

  describe "wake routing after a wildcard refresh" do
    # A queue created after boot reaches a wildcard fork's W routing as soon
    # as the refresh adds it to the union — the routing check for wildcard
    # forks is unconditional, so no routing-table update is needed.
    it "wakes a wildcard fork for a channel added by refresh" do
      config.workers = [{ queues: %w[*], threads: 1 }]
      allow(Pgbus::Process::WildcardQueueResolver).to receive(:resolve).and_return(%w[default])
      hub.start
      reader, writer = make_pipe
      hub.register_fork(pid: 300, queues: [], wildcard: true, pipe: writer)
      drain(reader)

      factory_calls.last[:on_wake].call("pgmq.q_pgbus_test_brand_new.INSERT")

      expect(drain(reader)).to eq("W")
      reader.close
      writer.close
    end
  end
end
