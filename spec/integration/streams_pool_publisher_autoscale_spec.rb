# frozen_string_literal: true

require_relative "../integration_helper"
require "concurrent"

# DB-gated proof that a PURE PUBLISHER (no streamer) grows its streams pool under
# a broadcast storm, via the throttled trigger baked into send_stream_message
# (issue #323 follow-up). No Streamer::Instance is created here — only a Client
# publishing. Auto-skips without PGBUS_DATABASE_URL. Modest sizes.
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
      c.streams_pool_timeout = 2
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

  it "grows the streams pool from the publish path under a concurrent broadcast storm" do
    # Saturate the streams pool with a churn of concurrent publishers. Each
    # send_stream_message fires the throttled trigger after producing.
    stop = Concurrent::AtomicBoolean.new(false)
    errors = Concurrent::Array.new
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

    # Sample the pool through the storm and track the peak size reached. A fast
    # publish INSERT holds a connection only briefly, so the pool grows into
    # headroom and (correctly) relaxes as the storm ebbs — the honest signal is
    # that it grew ABOVE the baseline at some point and never exceeded the cap.
    peak = 3
    20.times do
      sleep 0.1
      size = client.streams_pool_stats[:size]
      peak = size if size > peak
      break if peak >= 8
    end
    stop.make_true
    publishers.each { |t| t.join(5) }

    expect(errors).to be_empty
    expect(peak).to be > 3           # the publish path grew the pool into headroom
    expect(peak).to be <= 8          # respected the hard cap
    expect(client.streams_swap_stats.swap_count).to be >= 1
  end
end
