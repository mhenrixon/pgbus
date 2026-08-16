# frozen_string_literal: true

require_relative "../integration_helper"
require "concurrent"

# DB-gated proof that a PURE PUBLISHER (no streamer) grows its streams pool under
# a broadcast storm, via the throttled trigger baked into send_stream_message
# (issue #323 follow-up). No Streamer::Instance is created here — only a Client
# publishing. Auto-skips without PGBUS_DATABASE_URL. Modest sizes.
#
# DETERMINISM (issue #400): growth requires busy_ratio >= 0.85, which at the
# 3-connection baseline means ALL 3 connections checked out at the instant the
# background check samples pool stats. Leaving that to publisher-thread timing
# is scheduler luck — flaky on saturated CI runners. Instead the spec HOLDS 2 of
# the 3 baseline connections checked out for the whole storm, so any single
# in-flight publish saturates the pool; the storm then only has to keep the last
# connection busy, which 8 hammering threads do near-continuously. The trigger
# fires AFTER produce returns (the triggering publish never counts toward its
# own check), so holding all 3 would deadlock publishing — 2 is the maximum.
RSpec.describe "Streams pool publisher autoscale", :integration do
  let(:database_url) { ENV.fetch("PGBUS_DATABASE_URL") }
  let(:stream_name)  { "pas_#{SecureRandom.hex(4)}" }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_passpec"
      c.default_queue = "default"
      c.logger = Logger.new(IO::NULL)
      c.streams_pool_size = 3
      c.streams_pool_max = 8
      # 5s, not 2s: with 2 connections held, publishers rotate through a single
      # connection — a slow runner's checkout waits must not brush the timeout
      # and pollute the errors assertion.
      c.streams_pool_timeout = 5
      c.streams_pool_autoscale = true
      # A short (but non-zero) interval so the storm triggers a few real checks
      # in-test without the 5-min production cadence. interval 0 would defeat the
      # debounce and let the pool flap grow↔shrink on every fast publish; the
      # throttle itself is unit-tested separately.
      c.streams_pool_autoscale_interval = 0.05
    end
  end

  let(:client) { Pgbus::Client.new(config, schema_ensured: true) }

  before { client.ensure_stream_queue(stream_name) }
  after  { client.close }

  def monotonic_now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  it "grows the streams pool from the publish path under a concurrent broadcast storm" do
    # Demand scaffolding: pin 2 of the 3 baseline connections as checked out.
    # with_streams_connection is private — reached via #send because it is the
    # only way to occupy streams-pool checkouts without booting a streamer; the
    # behavior under test still flows through the public publish path.
    held = Concurrent::CountDownLatch.new(2)
    release = Queue.new
    holders = Array.new(2) do
      Thread.new do
        client.send(:with_streams_connection) do |_conn|
          held.count_down
          release.pop # nil on Queue#close — the cleanup wake-up
        end
      end
    end

    stop = Concurrent::AtomicBoolean.new(false)
    errors = Concurrent::Array.new
    publishers = []
    peak = 3
    begin
      expect(held.wait(5)).to be(true) # both holds established before the storm

      # Saturate the remaining connection with a churn of concurrent publishers.
      # Each send_stream_message fires the throttled trigger after producing.
      publishers = Array.new(8) do
        Thread.new do
          until stop.true?
            begin
              client.send_stream_message(stream_name, { "html" => "<x/>" })
            rescue StandardError => e
              errors << e
            end
          end
        end
      end

      # Bounded growth window instead of a blind sample count: at the 0.05s
      # autoscale interval this deadline covers ~300 check windows, and the loop
      # breaks the moment growth lands (ResizablePool#swap publishes the new pool
      # ref BEFORE draining the old one, so stats reflect it immediately).
      deadline = monotonic_now + 15
      while monotonic_now < deadline
        # streams_pool_stats returns {} on a transient read hiccup (it rescues
        # internally), and this loop runs concurrently with the swap storm — so
        # size can be nil. Skip those samples rather than raise NoMethodError.
        size = client.streams_pool_stats[:size]
        peak = size if size && size > peak
        break if peak > 3

        sleep 0.05
      end
    ensure
      # Always unwind, even when an expectation above raises — otherwise the
      # holders block on release.pop forever, pinning pool checkouts while the
      # after-hook closes the pool under them. Holders wake FIRST: they pin the
      # old pool's in-flight counter, and the post-grow drain (bounded at
      # streams_pool_timeout + 1) waits on it.
      stop.make_true
      release.close
      holders.each { |t| t.join(5) }
      publishers.each { |t| t.join(5) }
    end

    expect(errors).to be_empty
    expect(peak).to be > 3           # the publish path grew the pool into headroom
    expect(peak).to be <= 8          # respected the hard cap
    # swap_count increments only after the old pool's bounded drain completes on
    # the trigger's executor thread — poll briefly rather than assert instantly.
    swap_deadline = monotonic_now + config.streams_pool_timeout + 2
    sleep 0.05 while client.streams_swap_stats.swap_count < 1 && monotonic_now < swap_deadline
    expect(client.streams_swap_stats.swap_count).to be >= 1
  end
end
