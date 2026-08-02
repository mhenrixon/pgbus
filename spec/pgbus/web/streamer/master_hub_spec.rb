# frozen_string_literal: true

require "spec_helper"
require "socket"
require "tmpdir"

RSpec.describe Pgbus::Web::Streamer::MasterHub do
  subject(:hub) do
    described_class.new(
      config: config,
      socket_path: socket_path,
      listener_factory: listener_factory,
      status_interval: 0.05,
      logger: logger
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.queue_prefix = "pgbus_test"
      c.database_url = "postgres://fake@localhost/fake"
    end
  end
  let(:tmpdir) { Dir.mktmpdir("pgbus-hub-spec") }
  let(:socket_path) { File.join(tmpdir, "hub.sock") }
  let(:logger) { Logger.new(IO::NULL) }

  let(:fake_listener) do
    instance_double(
      Pgbus::Web::Streamer::Listener,
      ensure_listening: :done, remove_listening: nil, stop: nil,
      alive?: true, connected?: true
    )
  end
  # Captures the dispatch queue the hub hands its listener, so specs can
  # inject WakeMessages as if NOTIFY fired.
  let(:captured) { {} }
  let(:listener_factory) do
    lambda do |dispatch_queue:|
      captured[:dispatch_queue] = dispatch_queue
      fake_listener
    end
  end

  after do
    hub.stop
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
  end

  def connect_worker
    UNIXSocket.new(socket_path)
  end

  def send_frame(sock, message)
    sock.write(Pgbus::Web::Streamer::HubProtocol.encode(message))
  end

  def read_frame(sock, timeout: 2)
    raise "no frame within #{timeout}s" unless sock.wait_readable(timeout)

    Pgbus::Web::Streamer::HubProtocol.read_frame(sock)
  end

  # Reads frames until one matches the type (status rebroadcasts interleave).
  def read_frame_of_type(sock, type, timeout: 2)
    deadline = Time.now + timeout
    while Time.now < deadline
      frame = read_frame(sock, timeout: timeout)
      return frame if frame && frame["t"] == type
    end
    raise "no #{type} frame within #{timeout}s"
  end

  def wake(queue, payload = nil)
    captured[:dispatch_queue] << Pgbus::Web::Streamer::Listener::WakeMessage.new(
      queue_name: queue, payload: payload
    )
  end

  describe "subscription lifecycle" do
    it "acks a sub after the listener actually LISTENs" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })

      expect(read_frame_of_type(worker, "ack")).to include("q" => "pgbus_test_chat")
      expect(fake_listener).to have_received(:ensure_listening).with("pgbus_test_chat")
      worker.close
    end

    it "registers the subscription BEFORE the LISTEN completes (no lost-wake gap)" do
      # A wake that fires between LISTEN-active and sub-registration would be
      # lost. Pin the ordering: block ensure_listening, inject a wake while
      # blocked, then release — the worker must still receive that wake.
      gate = Queue.new
      allow(fake_listener).to receive(:ensure_listening) do |_q|
        gate.pop
        :done
      end
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      sleep 0.1 # let the sub reach the blocked ensure_listening
      wake("pgbus_test_chat", nil)
      gate << :go

      # The wake delivered while LISTEN was still in flight precedes the ack
      # in the outbox FIFO — collect both (status rebroadcasts interleave).
      seen = {}
      until seen.key?("ack") && seen.key?("wake")
        frame = read_frame(worker)
        seen[frame["t"]] = frame unless frame["t"] == "status"
      end
      expect(seen["ack"]).to include("q" => "pgbus_test_chat")
      expect(seen["wake"]).to include("q" => "pgbus_test_chat")
      worker.close
    end

    it "acks every subscriber through the listener (idempotent) but UNLISTENs only at zero refs" do
      # Each sub must round-trip ensure_listening so ITS ack carries the
      # LISTEN-active guarantee (a refcount shortcut would ack subscriber B
      # while subscriber A's LISTEN was still in flight — reopening the
      # lost-wake gap). ensure_listening is cheap when already listening.
      hub.start
      worker_a = connect_worker
      worker_b = connect_worker
      send_frame(worker_a, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker_a, "ack")
      send_frame(worker_b, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker_b, "ack")

      expect(fake_listener).to have_received(:ensure_listening).with("pgbus_test_chat").twice

      send_frame(worker_a, { "t" => "unsub", "q" => "pgbus_test_chat" })
      sleep 0.1
      expect(fake_listener).not_to have_received(:remove_listening)

      send_frame(worker_b, { "t" => "unsub", "q" => "pgbus_test_chat" })
      sleep 0.1
      expect(fake_listener).to have_received(:remove_listening).with("pgbus_test_chat")
      [worker_a, worker_b].each(&:close)
    end

    it "releases a dead worker's subscriptions on EOF" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker, "ack")

      worker.close
      sleep 0.2

      expect(fake_listener).to have_received(:remove_listening).with("pgbus_test_chat")
    end
  end

  describe "wake fanout" do
    it "routes wakes only to subscribed workers, payload intact" do
      hub.start
      worker_a = connect_worker
      worker_b = connect_worker
      send_frame(worker_a, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker_a, "ack")
      send_frame(worker_b, { "t" => "sub", "q" => "pgbus_test_other" })
      read_frame_of_type(worker_b, "ack")

      wake("pgbus_test_chat", "<div>ephemeral</div>")

      frame = read_frame_of_type(worker_a, "wake")
      expect(frame).to include("q" => "pgbus_test_chat", "p" => "<div>ephemeral</div>")
      expect(worker_b.wait_readable(0.3)).to be_falsey.or(satisfy do |r|
        # Only status frames may arrive on B; never a wake for chat.
        r && Pgbus::Web::Streamer::HubProtocol.read_frame(worker_b)["t"] != "wake"
      end)
      [worker_a, worker_b].each(&:close)
    end

    it "delivers durable wakes with a null payload" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker, "ack")

      wake("pgbus_test_chat", nil)

      expect(read_frame_of_type(worker, "wake")).to include("q" => "pgbus_test_chat", "p" => nil)
      worker.close
    end
  end

  describe "backpressure" do
    subject(:hub) do
      described_class.new(
        config: config, socket_path: socket_path, listener_factory: listener_factory,
        status_interval: 60, durable_queue_limit: 3, hard_queue_limit: 8, logger: logger
      )
    end

    it "drops excess durable wakes for a non-draining worker but keeps ephemeral" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      sleep 0.1
      # Wedge the writer below the hard cap: a few LARGE ephemeral frames
      # fill the kernel socket buffer (worker never reads), blocking the
      # writer mid-write with the outbox well under hard_queue_limit(8).
      3.times { wake("pgbus_test_chat", "x" * 262_144) }
      sleep 0.2
      # Durable wakes now pile into the outbox: droppable beyond limit 3.
      20.times { wake("pgbus_test_chat", nil) }
      sleep 0.2

      expect(hub.dropped_durable_wakes).to be > 0
      expect(hub.evicted_workers).to eq(0)
      worker.close
    end

    it "evicts a worker whose queue exceeds the hard cap (its fallback takes over)" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      sleep 0.1
      # Ephemeral frames are never dropped, so they push past the hard cap →
      # eviction severs the socket.
      200.times { wake("pgbus_test_chat", "x" * 65_536) }

      deadline = Time.now + 5
      severed = false
      while Time.now < deadline
        begin
          worker.read_nonblock(1_048_576)
        rescue IO::WaitReadable
          sleep 0.05
        rescue EOFError, Errno::ECONNRESET
          severed = true
          break
        end
      end
      expect(severed).to be true
      expect(hub.evicted_workers).to eq(1)
    end
  end

  describe "status broadcast" do
    it "broadcasts degraded and healthy transitions" do
      hub.start
      worker = connect_worker
      send_frame(worker, { "t" => "sub", "q" => "pgbus_test_chat" })
      read_frame_of_type(worker, "ack")

      allow(fake_listener).to receive(:connected?).and_return(false)
      frame = read_frame_of_type(worker, "status", timeout: 3)
      expect(frame).to include("healthy" => false)

      allow(fake_listener).to receive(:connected?).and_return(true)
      frame = read_frame_of_type(worker, "status", timeout: 3)
      expect(frame).to include("healthy" => true)
      worker.close
    end
  end

  describe "#stop" do
    it "stops the listener, closes clients, and unlinks the socket" do
      hub.start
      worker = connect_worker

      hub.stop

      expect(fake_listener).to have_received(:stop)
      expect(File.exist?(socket_path)).to be false
      expect(worker.wait_readable(1) && Pgbus::Web::Streamer::HubProtocol.read_frame(worker)).to be_nil
      worker.close
    end

    it "replaces a stale socket file on start" do
      File.write(socket_path, "stale")
      expect { hub.start }.not_to raise_error
      expect(File.socket?(socket_path)).to be true
    end
  end
end
