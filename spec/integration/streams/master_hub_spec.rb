# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #382 acceptance against real PostgreSQL + real LISTEN/NOTIFY:
#   - one census-tagged LISTEN connection (the MasterHub's) serves multiple
#     "workers" connected over the Unix socket
#   - real ephemeral NOTIFY payloads ride the frames intact
#   - killing the shared LISTEN backend is survived (listener reconnect,
#     wakes flow again)
#   - a worker whose master DIES fails over to its own listener and keeps
#     receiving wakes (the settled fallback: connections over loss)
RSpec.describe "Streams master hub (issue #382)", :integration do
  let(:config) { Pgbus.configuration }
  let(:logger) { Logger.new(IO::NULL) }
  let(:tmpdir) { Dir.mktmpdir("pgbus-hub-int") }
  let(:socket_path) { File.join(tmpdir, "hub.sock") }
  let(:stream_name) { "hubint_#{SecureRandom.hex(4)}" }
  let(:physical) { config.queue_name(stream_name) }

  around do |example|
    saved = config.listen_notify
    config.listen_notify = true
    example.run
  ensure
    config.listen_notify = saved
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
  end

  before { Pgbus.client.ensure_stream_queue(stream_name) }

  def wait_until(timeout: 10)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.05
    end
  end

  def send_frame(sock, message)
    sock.write(Pgbus::Web::Streamer::HubProtocol.encode(message))
  end

  def read_frame_of_type(sock, type, timeout: 5)
    deadline = Time.now + timeout
    while Time.now < deadline
      raise "no #{type} frame within #{timeout}s" unless sock.wait_readable(timeout)

      frame = Pgbus::Web::Streamer::HubProtocol.read_frame(sock)
      raise "peer closed while waiting for #{type}" if frame.nil?
      return frame if frame["t"] == type
    end
    raise "no #{type} frame within #{timeout}s"
  end

  def listen_backend_pids
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT pid FROM pg_stat_activity
      WHERE application_name = 'pgbus-listen' AND datname = current_database()
    SQL
  end

  it "serves workers over ONE connection, carries ephemeral payloads, survives a backend kill" do
    baseline_pids = listen_backend_pids
    hub = Pgbus::Web::Streamer::MasterHub.new(
      config: config, socket_path: socket_path, status_interval: 0.5, logger: logger
    )
    worker = nil
    begin
      hub.start
      wait_until { (listen_backend_pids - baseline_pids).size == 1 }

      worker = UNIXSocket.new(socket_path)
      send_frame(worker, { "t" => "sub", "q" => physical })
      read_frame_of_type(worker, "ack")

      # Census: the whole host still pins exactly ONE streams connection.
      expect((listen_backend_pids - baseline_pids).size).to eq(1)

      # A real ephemeral broadcast (pg_notify with payload) rides the frame.
      Pgbus.client.notify_stream(stream_name, "<div>ephemeral hello</div>")
      frame = read_frame_of_type(worker, "wake")
      expect(frame["q"]).to eq(physical)
      expect(frame["p"]).to include("ephemeral hello")

      # Chaos: kill the shared LISTEN backend; the listener reconnects and
      # wakes flow again.
      old_pids = listen_backend_pids - baseline_pids
      ActiveRecord::Base.connection.execute(<<~SQL)
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE pid IN (#{old_pids.join(",")})
      SQL
      wait_until do
        fresh = listen_backend_pids - baseline_pids
        !fresh.empty? && !fresh.intersect?(old_pids)
      end
      sleep 0.3
      Pgbus.client.notify_stream(stream_name, "<div>after recovery</div>")
      frame = read_frame_of_type(worker, "wake", timeout: 10)
      expect(frame["p"]).to include("after recovery")
    ensure
      worker&.close
      hub.stop
    end
  end

  it "a worker fails over to its OWN listener when the master dies, without losing wakes" do
    hub = Pgbus::Web::Streamer::MasterHub.new(
      config: config, socket_path: socket_path, status_interval: 0.5, logger: logger
    )
    dispatch_queue = Queue.new
    failover = nil
    begin
      hub.start
      wait_until { File.socket?(socket_path) }

      client = Pgbus::Web::Streamer::HubClient.new(
        socket_path: socket_path, dispatch_queue: dispatch_queue,
        ack_timeout: 5, on_failure: -> { failover&.fail_over! }, logger: logger
      )
      client.connect
      failover = Pgbus::Web::Streamer::FailoverListener.new(
        hub_client: client,
        local_listener_factory: lambda {
          conn_factory = -> { Pgbus::DedicatedConnection.connect(config.streams_connection_options) }
          Pgbus::Web::Streamer::Listener.new(
            pg_connection: conn_factory.call,
            dispatch_queue: dispatch_queue,
            health_check_ms: 250,
            connection_factory: conn_factory,
            logger: logger
          ).tap(&:start)
        },
        logger: logger
      )

      failover.ensure_listening(physical)
      Pgbus.client.notify_stream(stream_name, "<div>via hub</div>")
      expect(dispatch_queue.pop(timeout: 5)&.payload).to include("via hub")

      # Master dies. The client EOFs, fail_over! builds a real per-worker
      # listener and re-LISTENs the recorded set.
      hub.stop
      wait_until(timeout: 5) { client.dead? }
      # ensure_listening after death exercises the sync failover path too.
      failover.ensure_listening(physical)

      sleep 0.3
      Pgbus.client.notify_stream(stream_name, "<div>via fallback</div>")
      message = dispatch_queue.pop(timeout: 10)
      message = dispatch_queue.pop(timeout: 10) while message && !message.payload&.include?("via fallback")
      expect(message&.payload).to include("via fallback")
    ensure
      failover&.stop
    end
  end
end
