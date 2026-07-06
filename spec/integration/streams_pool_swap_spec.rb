# frozen_string_literal: true

require_relative "../integration_helper"
require "concurrent"

# DB-gated proof that Client#resize_streams_pool hot-swaps the streams pool under
# live load with ZERO lost durable broadcasts and ZERO leaked connections
# (issue #323 spike). Auto-skips without PGBUS_DATABASE_URL. Small/fast.
RSpec.describe "Streams pool hot-swap", :integration do
  let(:database_url) { ENV.fetch("PGBUS_DATABASE_URL") }
  let(:stream_name)  { "swp_#{SecureRandom.hex(4)}" }

  let(:client) do
    config = Pgbus::Configuration.new.tap do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_swapspec"
      c.default_queue = "default"
      c.logger = Logger.new(IO::NULL)
      c.streams_pool_size = 3
      c.streams_pool_timeout = 5
    end
    Pgbus::Client.new(config, schema_ensured: true)
  end

  before { client.ensure_stream_queue(stream_name) }
  after  { client.close }

  def landed_count
    full = client.config.queue_name(stream_name)
    sanitized = Pgbus::QueueNameValidator.sanitize!(full)
    conn = PG.connect(database_url)
    conn.exec("SELECT count(*) AS n FROM pgmq.q_#{sanitized}").first["n"].to_i
  ensure
    conn&.close
  end

  it "loses zero durable broadcasts across a grow swap under concurrent load" do
    produced = Concurrent::AtomicFixnum.new(0)
    errors   = Concurrent::Array.new
    stop     = Concurrent::AtomicBoolean.new(false)

    producers = Array.new(3) do
      Thread.new do
        until stop.true?
          begin
            client.send_stream_message(stream_name, { "html" => "<x/>" })
            produced.increment
          rescue StandardError => e
            errors << e
          end
        end
      end
    end

    sleep 0.1 # warm up some load
    client.resize_streams_pool(8) # grow 3 -> 8 mid-load
    sleep 0.1
    stop.make_true
    producers.each { |t| t.join(5) }

    expect(errors).to be_empty # in particular, no "Connection pool is closed"
    expect(landed_count).to eq(produced.value) # every successful produce is in the queue
    expect(client.streams_pool_stats[:size]).to eq(8)
    expect(client.streams_swap_stats.swap_count).to eq(1)
  end

  it "leaks zero connections across grow then shrink" do
    baseline = pg_backend_count
    client.send_stream_message(stream_name, { "html" => "<x/>" }) # open the pool

    client.resize_streams_pool(8)
    client.resize_streams_pool(3)
    sleep 0.2 # let the old pools' connections finish closing

    expect(client.streams_pool_stats[:size]).to eq(3)
    expect(pg_backend_count).to be <= baseline + 3 # only the final 3-slot pool remains
  end

  # Connections owned by THIS spec's client (its queue_prefix appears in the app
  # query text or the pool's application_name is unset, so scope by database +
  # the swapspec queue prefix in the query text is unreliable; count total on the
  # test DB and assert non-growth).
  def pg_backend_count
    conn = PG.connect(database_url)
    conn.exec("SELECT count(*) AS n FROM pg_stat_activity").first["n"].to_i
  ensure
    conn&.close
  end
end
