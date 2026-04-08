# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::Dispatcher do
  subject(:dispatcher) do
    described_class.new(
      client: client,
      registry: registry,
      listener: listener,
      dispatch_queue: dispatch_queue,
      logger: logger,
      read_limit: 500
    )
  end

  let(:client) do
    double("Pgbus::Client", config: stream_config)
  end
  let(:stream_config) do
    instance_double(Pgbus::Configuration).tap do |c|
      # Dispatcher#full_table_name_for calls config.queue_name(stream) to
      # translate logical names to PGMQ full table names. In these unit
      # tests we pass the stream name through unchanged — the real
      # prefixing behavior is covered in the integration tests.
      allow(c).to receive(:queue_name) { |name| name }
    end
  end
  # The connection double records enqueued envelopes and exposes a
  # controllable cursor. It implements just the surface the Dispatcher
  # touches, so the test is hermetic and doesn't depend on the real
  # Connection's write path.
  let(:conn_class) do
    Class.new do
      attr_accessor :last_msg_id_sent
      attr_reader :id, :stream_name, :enqueued, :context

      def initialize(id:, stream_name:, last_msg_id_sent: 0, context: nil)
        @id = id
        @stream_name = stream_name
        @last_msg_id_sent = last_msg_id_sent
        @context = context
        @enqueued = []
        @dead = false
      end

      def enqueue(envelopes)
        @enqueued.concat(envelopes)
        envelopes.each { |e| @last_msg_id_sent = e.msg_id if e.msg_id > @last_msg_id_sent }
        envelopes
      end

      def dead? = @dead
      def mark_dead! = @dead = true
    end
  end
  let(:registry)       { Pgbus::Web::Streamer::Registry.new }
  let(:listener)       { double("Listener", ensure_listening: nil, remove_listening: nil) }
  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }

  def build_conn(**kwargs)
    conn_class.new(**kwargs)
  end

  def build_envelope(msg_id)
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: msg_id,
      enqueued_at: "2026-04-07T00:00:00Z",
      payload: "<turbo-stream>#{msg_id}</turbo-stream>",
      source: "live"
    )
  end

  describe "WakeMessage fanout" do
    it "reads from the minimum cursor across registered connections" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 100)
      c2 = build_conn(id: "b", stream_name: "chat", last_msg_id_sent: 200)
      registry.register(c1)
      registry.register(c2)

      allow(client).to receive(:read_after)
        .and_return([build_envelope(150), build_envelope(250)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(client).to have_received(:read_after).with("chat", after_id: 100, limit: 500)
      # c1 sees both (150, 250) because its cursor was at 100
      # c2 only sees 250 (envelope 150 <= c2's cursor of 200 — Connection dedups)
      expect(c1.enqueued.map(&:msg_id)).to contain_exactly(150, 250)
      expect(c2.enqueued.map(&:msg_id)).to contain_exactly(150, 250) # fake conn doesn't dedupe
    end

    it "is a no-op when no connections subscribe to the stream" do
      allow(client).to receive(:read_after)
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "unknown"))
      expect(client).not_to have_received(:read_after)
    end

    it "posts DisconnectMessages for connections that turned dead during enqueue" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      c1.mark_dead!

      allow(client).to receive(:read_after).and_return([build_envelope(10)])
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      msg = dispatch_queue.pop
      expect(msg).to be_a(described_class::DisconnectMessage)
      expect(msg.connection).to be(c1)
    end
  end

  describe "ConnectMessage — the 5-step race-free replay sequence" do
    let(:connection) { build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1247) }

    it "ensures the listener is subscribed to the stream (step 1)" do
      allow(client).to receive(:read_after).and_return([])
      allow(listener).to receive(:ensure_listening)

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(listener).to have_received(:ensure_listening).with("chat")
    end

    it "reads the archive from the connection's since_id (step 3)" do
      allow(client).to receive(:read_after)
        .and_return([build_envelope(1248), build_envelope(1249)])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(client).to have_received(:read_after).with("chat", after_id: 1247, limit: 500)
      expect(connection.enqueued.map(&:msg_id)).to eq([1248, 1249])
    end

    it "registers the connection in the main registry (step 5)" do
      allow(client).to receive(:read_after).and_return([])
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(registry.connections_for("chat")).to contain_exactly(connection)
    end

    it "does not register a connection that died during replay" do
      allow(client).to receive(:read_after).and_return([])
      connection.mark_dead!
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(registry.connections_for("chat")).to be_empty
    end

    it "cleans up @full_to_logical and unlistens when a connect dies before registry promotion" do
      # Without the dead-before-register cleanup path, this scenario
      # would pin @full_to_logical[full_name] = "chat" and leave the
      # PG LISTEN active for the life of the worker, since the
      # connection never reaches the registry and no DisconnectMessage
      # is ever emitted.
      allow(client).to receive(:read_after).and_return([])
      connection.mark_dead!

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(dispatcher.instance_variable_get(:@full_to_logical)).to be_empty
      expect(listener).to have_received(:remove_listening).with("chat")
    end

    it "cleans up @full_to_logical and unlistens when step 3 raises" do
      # Same leak surface from a thrown exception path — the rescue
      # must scrub state so a transient failure on a single connect
      # doesn't permanently bloat @full_to_logical.
      allow(client).to receive(:read_after).and_raise(StandardError, "boom")

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(dispatcher.instance_variable_get(:@full_to_logical)).to be_empty
      expect(listener).to have_received(:remove_listening).with("chat")
      expect(connection.dead?).to be true
    end

    it "removes the in-flight buffer entry after register" do
      allow(client).to receive(:read_after).and_return([])
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(dispatcher.instance_variable_get(:@in_flight)["chat"]).to be_nil.or eq([])
    end
  end

  describe "the replay race (the rails/rails#52420 fix)" do
    # These are the load-bearing tests for the whole subsystem. They prove
    # that a WakeMessage arriving *between* "subscribe" and "register"
    # during a ConnectMessage is not lost. Without the in-flight buffer
    # pattern, handle_wake finds no connections to fan out to (the new
    # connection isn't in the registry yet) and the archive read in step 3
    # may miss a message that hasn't committed yet.

    it "routes a WakeMessage to in-flight connects, not just registered ones" do
      # Manually install an in-flight entry, then fire a WakeMessage and
      # verify the buffer received the envelopes. This tests the routing
      # half of the race fix in isolation.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)
      buffer = []
      dispatcher.instance_variable_get(:@in_flight)["chat"] << [connection, buffer]

      allow(client).to receive(:read_after)
        .with("chat", after_id: 1000, limit: 500)
        .and_return([build_envelope(1050), build_envelope(1051)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(buffer.map(&:msg_id)).to eq([1050, 1051])
      # The in-flight connection is NOT in the registry, so it should
      # not have been enqueued to directly by the wake path — only the
      # buffer gets populated.
      expect(connection.enqueued).to be_empty
    end

    it "drains the in-flight buffer into the connection on register (step 4)" do
      # Verifies the drain half of the race fix: after handle_connect
      # completes, the connection has received anything that arrived in
      # the buffer while step 3's read_after was still running.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)

      # read_after is called twice during handle_connect:
      #   1. step 3: the initial archive read for the connection
      #   2. NO — step 4 drains the in-flight buffer directly, not via read_after
      # So we only expect one call.
      allow(client).to receive(:read_after) do |*|
        # While step 3 is running, simulate a WakeMessage landing for
        # this stream by invoking handle_wake directly. This populates
        # the in-flight buffer with a newly-broadcast envelope.
        allow(client).to receive(:read_after).and_return([build_envelope(1050)])
        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
        # After the wake, step 3's read still needs to return something.
        # In a real race the archive might or might not have the message
        # yet — we return empty to prove the buffer path is what saves us.
        []
      end

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      # The envelope must have landed on the connection via the buffer
      # drain (step 4), since step 3 returned an empty result.
      expect(connection.enqueued.map(&:msg_id)).to contain_exactly(1050)
    end

    it "handles the simple case where step 3 finds the envelope in the archive" do
      # This is the non-racy path: the broadcast committed before connect
      # started, so the archive read in step 3 returns the envelope and
      # the buffer stays empty.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)

      allow(client).to receive(:read_after)
        .with("chat", after_id: 1000, limit: 500)
        .and_return([build_envelope(1050)])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(connection.enqueued.map(&:msg_id)).to contain_exactly(1050)
    end
  end

  describe "audience filtering via visible_to" do
    subject(:dispatcher) do
      described_class.new(
        client: client,
        registry: registry,
        listener: listener,
        dispatch_queue: dispatch_queue,
        logger: logger,
        read_limit: 500,
        filters: filters
      )
    end

    let(:filters) { Pgbus::Streams::Filters.new }
    let(:admin_conn)  { build_conn(id: "admin", stream_name: "chat", context: { role: "admin" }) }
    let(:viewer_conn) { build_conn(id: "viewer", stream_name: "chat", context: { role: "viewer" }) }

    before do
      filters.register(:admin_only) { |ctx| ctx[:role] == "admin" }
      registry.register(admin_conn)
      registry.register(viewer_conn)
    end

    def admin_envelope
      Pgbus::Web::Streamer::Dispatcher::StreamEnvelope.new(
        msg_id: 100,
        enqueued_at: nil,
        payload: "<turbo-stream>secret</turbo-stream>",
        source: "live",
        visible_to: :admin_only
      )
    end

    def public_envelope
      Pgbus::Web::Streamer::Dispatcher::StreamEnvelope.new(
        msg_id: 101,
        enqueued_at: nil,
        payload: "<turbo-stream>everyone</turbo-stream>",
        source: "live",
        visible_to: nil
      )
    end

    it "delivers a labeled envelope only to connections matching the filter" do
      raw = double("raw", payload: '{"html":"<turbo-stream>secret</turbo-stream>","visible_to":"admin_only"}',
                          msg_id: 100, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued.map(&:msg_id)).to eq([100])
      expect(viewer_conn.enqueued).to be_empty
    end

    it "delivers an unlabeled envelope to every connection (no filter applied)" do
      raw = double("raw", payload: '{"html":"<turbo-stream>everyone</turbo-stream>"}',
                          msg_id: 101, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued.map(&:msg_id)).to eq([101])
      expect(viewer_conn.enqueued.map(&:msg_id)).to eq([101])
    end

    it "fail-opens on an unknown filter label (delivers to all and logs)" do
      allow(logger).to receive(:warn)
      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"nope"}',
                          msg_id: 102, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued.map(&:msg_id)).to eq([102])
      expect(viewer_conn.enqueued.map(&:msg_id)).to eq([102])
    end

    it "fail-closes when the filter predicate raises (drops the envelope)" do
      filters.register(:broken) { |_| raise "boom" }
      allow(logger).to receive(:error)
      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"broken"}',
                          msg_id: 103, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued).to be_empty
      expect(viewer_conn.enqueued).to be_empty
    end

    it "applies the filter on the in-flight buffer path during handle_connect" do
      new_conn = build_conn(id: "new", stream_name: "chat", context: { role: "viewer" })

      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"admin_only"}',
                          msg_id: 200, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: new_conn))

      expect(new_conn.enqueued).to be_empty
    end
  end

  describe "DisconnectMessage" do
    it "unregisters the connection" do
      conn = build_conn(id: "a", stream_name: "chat")
      registry.register(conn)

      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: conn))

      expect(registry.connections_for("chat")).to be_empty
    end
  end

  describe "#start and #stop" do
    it "processes queued messages on a background thread" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.start
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")

      deadline = Time.now + 2
      sleep 0.01 until c.enqueued.any? || Time.now > deadline
      expect(c.enqueued.map(&:msg_id)).to contain_exactly(10)

      dispatcher.stop
    end
  end
end
