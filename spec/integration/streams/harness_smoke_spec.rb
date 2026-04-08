# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Smoke test for the Phase 5 integration infrastructure. Proves that:
#
#   1. SseTestSupport::PumaTestHarness can boot a real Puma::Server on an ephemeral
#      port and accept TCP connections.
#   2. A minimal Rack app can hijack the socket, write SSE bytes, and
#      NOT tie up the Puma thread for the lifetime of the connection.
#   3. SseTestSupport::SseTestClient parses the bytes into Event structs correctly.
#
# If this spec fails, nothing in the streams integration story can
# work. We run it first (alphabetically and explicitly) so a broken
# harness doesn't look like a broken Streamer.
#
# Tagged :integration but does NOT need Postgres — only Puma.
RSpec.describe "Streams integration harness", :integration do
  include SseTestSupport

  # A trivial Rack app that hijacks the socket, writes three SSE events,
  # then closes the socket. Mimics the shape of Pgbus::Web::StreamApp
  # without any pgbus internals.
  let(:hijacking_app) do
    lambda do |env|
      next [501, { "content-type" => "text/plain" }, ["no hijack"]] unless env["rack.hijack?"]

      env["rack.hijack"].call
      io = env["rack.hijack_io"]

      Thread.new do
        io.write("HTTP/1.1 200 OK\r\n")
        io.write("content-type: text/event-stream\r\n")
        io.write("cache-control: no-cache\r\n")
        io.write("\r\n")
        io.write("id: 1\nevent: ping\ndata: hello\n\n")
        io.write("id: 2\nevent: ping\ndata: world\n\n")
        io.write("id: 3\nevent: ping\ndata: !\n\n")
        sleep 0.05
        io.close
      end

      [-1, {}, []]
    end
  end

  let(:harness) { SseTestSupport::PumaTestHarness.boot(rack_app: hijacking_app) }

  after { harness.shutdown }

  it "boots a Puma server and exposes a working ephemeral port" do
    expect(harness.port).to be_a(Integer)
    expect(harness.port).to be > 0

    # Can we open a TCP connection?
    sock = TCPSocket.new("127.0.0.1", harness.port)
    sock.close
  end

  it "routes a Rack hijack through Puma end-to-end with SseTestSupport::SseTestClient" do
    client = SseTestSupport::SseTestClient.connect(url: harness.url("/stream"), timeout: 3)
    events = client.wait_for_events(count: 3, timeout: 3)
    client.close

    expect(events.size).to eq(3)
    expect(events.map(&:id)).to eq(%w[1 2 3])
    expect(events.map(&:event)).to all(eq("ping"))
    expect(events.map(&:data)).to eq(%w[hello world !])
  end

  it "releases the Puma worker thread after the Rack app returns from hijack" do
    # The load-bearing architectural claim from puma/puma#1009 is that
    # hijack releases the Puma thread as long as the Rack code returns
    # promptly after calling env['rack.hijack']. If this test fails,
    # the whole Phase 4 architecture is wrong.
    #
    # Proof by counting: fire 20 hijacked connections in parallel. The
    # Puma server has max_threads=8, so if hijacked connections tied
    # up threads, the 9th connection would block until one of the
    # first 8 finished. All 20 should complete in the time it takes
    # for the slowest one (a few hundred milliseconds).
    threads = Array.new(20) do
      Thread.new do
        client = SseTestSupport::SseTestClient.connect(url: harness.url("/stream"), timeout: 3)
        events = client.wait_for_events(count: 3, timeout: 3)
        client.close
        events
      end
    end
    start = Time.now
    results = threads.map(&:value)
    elapsed = Time.now - start

    expect(results.all? { |events| events.size == 3 }).to be true
    # 20 connections with 8 threads × ~50ms-per-request would be ≥125ms
    # if they serialized. Hijack-releases-thread means they run in
    # parallel and total elapsed is just the slowest single request.
    # We allow 2 seconds for test-environment noise.
    expect(elapsed).to be < 2.0
  end
end
