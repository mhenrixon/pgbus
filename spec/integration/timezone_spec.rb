# frozen_string_literal: true

require_relative "../integration_helper"

# Reproduces timezone-related issues when Rails is configured with:
# - ActiveRecord::Base.default_timezone = :local
# - Time.zone set to a non-UTC timezone (e.g. Africa/Johannesburg, UTC+2)
# - System timezone (ENV['TZ']) set to the same non-UTC timezone
#
# This combination is common in apps that store timestamps in local time
# (e.g. getzazu/app using Africa/Casablanca). The root issue is that pgbus
# mixes Time.now.utc, Time.now, and Time.current inconsistently, which
# produces wrong values when AR normalizes through :local timezone handling.
RSpec.describe "Timezone handling (integration)", :integration do # rubocop:disable RSpec/DescribeClass
  around do |example|
    original_env_tz = ENV["TZ"]
    original_ar_tz = ActiveRecord::Base.default_timezone
    original_zone = Time.zone

    # Simulate a non-UTC Rails app: system TZ, AR default_timezone, and
    # Rails Time.zone all set to Africa/Johannesburg (UTC+2).
    ENV["TZ"] = "Africa/Johannesburg"
    ActiveRecord::Base.default_timezone = :local
    Time.zone = "Africa/Johannesburg"

    # Force PG session timezone to match the new setting. Rails normally
    # does this on connection establishment, but since we're reusing an
    # existing connection we need to do it manually.
    ActiveRecord::Base.connection.execute(
      "SET SESSION timezone = 'Africa/Johannesburg'"
    )

    example.run
  ensure
    ENV["TZ"] = original_env_tz
    ActiveRecord::Base.default_timezone = original_ar_tz
    Time.zone = original_zone
    ActiveRecord::Base.connection.execute("SET SESSION timezone = 'UTC'")
  end

  describe "Semaphore expiration" do
    before { Pgbus::Semaphore.delete_all }

    it "correctly expires semaphores when using non-UTC timezone" do
      # Acquire with a very short duration (already expired)
      Pgbus::Concurrency::Semaphore.acquire("tz-expired-key", 1, -1)

      # Acquire with a long duration (should NOT expire)
      Pgbus::Concurrency::Semaphore.acquire("tz-active-key", 1, 300)

      # expire_stale should correctly identify which is expired
      expired = Pgbus::Concurrency::Semaphore.expire_stale
      expired_keys = expired.map { |r| r["key"] }

      expect(expired_keys).to include("tz-expired-key")
      expect(expired_keys).not_to include("tz-active-key")
    end

    it "does not prematurely expire active semaphores due to timezone offset" do
      # Acquire with 60-second duration — should NOT expire immediately
      Pgbus::Concurrency::Semaphore.acquire("tz-test-key", 1, 60)

      expired = Pgbus::Concurrency::Semaphore.expire_stale
      expired_keys = expired.map { |r| r["key"] }

      expect(expired_keys).not_to include("tz-test-key")
      expect(Pgbus::Concurrency::Semaphore.current_value("tz-test-key")).to eq(1)
    end

    it "stores expires_at that round-trips correctly through AR" do
      Pgbus::Concurrency::Semaphore.acquire("tz-roundtrip", 1, 120)

      record = Pgbus::Semaphore.find_by(key: "tz-roundtrip")
      now = Time.current

      # expires_at should be ~120 seconds from now, not offset by timezone hours
      delta = record.expires_at - now
      expect(delta).to be_within(5).of(120)
    end
  end

  describe "Blocked execution expiration" do
    before { Pgbus::BlockedExecution.delete_all }

    it "correctly identifies expired blocked executions" do
      # Insert a blocked execution that's already expired
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-key", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: -1
      )

      count = Pgbus::Concurrency::BlockedExecution.expire_stale
      expect(count).to eq(1)
    end

    it "does not prematurely expire active blocked executions" do
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-active-key", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: 300
      )

      count = Pgbus::Concurrency::BlockedExecution.expire_stale
      expect(count).to eq(0)
    end

    it "stores expires_at that round-trips correctly through AR" do
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-roundtrip", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: 120
      )

      record = Pgbus::BlockedExecution.find_by(concurrency_key: "tz-roundtrip")
      now = Time.current

      # expires_at should be ~120 seconds from now, not offset by timezone hours
      delta = record.expires_at - now
      expect(delta).to be_within(5).of(120)
    end

    it "releases blocked executions whose expires_at has not passed" do
      # Insert a blocked execution with 5-minute duration
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-release-key", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: 300
      )

      # release_next! should find it (expires_at is in the future)
      released = Pgbus::BlockedExecution.release_next!("tz-release-key")
      expect(released).not_to be_nil
      expect(released[:queue_name]).to eq("default")
    end
  end

  describe "Job enqueue with delay" do
    let(:client) { Pgbus.client }

    before { client.ensure_queue("default") }

    it "correctly calculates delay from a timezone-aware timestamp" do
      adapter = Pgbus::ActiveJob::Adapter.new
      job_id = SecureRandom.uuid
      job = double("ActiveJob",
                    queue_name: "default",
                    job_id: job_id,
                    scheduled_at: nil)
      allow(job).to receive_messages(provider_job_id: nil)
      allow(job).to receive(:provider_job_id=)
      allow(job).to receive(:class).and_return(Class.new)
      allow(job).to receive(:perform_now)

      serialized = {
        "job_class" => "TestJob",
        "job_id" => job_id,
        "queue_name" => "default",
        "arguments" => []
      }
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized)

      # Schedule 5 seconds from now using Time.current (Rails timezone)
      future_timestamp = Time.current.to_f + 5

      adapter.enqueue_at(job, future_timestamp)

      # The message should be invisible (delayed) — reading immediately should
      # return nothing because the VT is set 5 seconds in the future
      messages = client.read_batch("default", qty: 1, vt: 1)
      expect(messages || []).to be_empty
    end
  end

  describe "Enqueue latency computation" do
    let(:mock_client) { build_mock_client }
    let(:config) { Pgbus.configuration }

    it "computes correct latency when enqueued_at is in local timezone" do
      executor = Pgbus::ActiveJob::Executor.new(client: mock_client, config: config)
      job_id = SecureRandom.uuid
      job_payload = {
        "job_class" => "TestJob", "job_id" => job_id,
        "queue_name" => "default", "arguments" => []
      }
      message_json = JSON.generate(job_payload)
      job_double = build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id)

      allow(ActiveJob::Base).to receive(:deserialize).and_return(job_double)
      allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
      stub_const("Pgbus::JobStat", Class.new) unless defined?(Pgbus::JobStat)
      allow(Pgbus::JobStat).to receive(:record!)

      # Simulate enqueued_at from a non-UTC PG session (no +00 suffix)
      # This is the key scenario: the timestamp string has NO timezone offset,
      # and the system is in Africa/Johannesburg (UTC+2). If Time.parse
      # interprets it as local time but the value is actually UTC, the
      # latency will be off by 2 hours (7200 seconds = 7_200_000 ms).
      one_second_ago_utc = (Time.now.utc - 1).strftime("%Y-%m-%d %H:%M:%S.%6N")
      message = build_message_double(
        msg_id: 1, message: message_json, read_ct: 1,
        enqueued_at: one_second_ago_utc
      )

      executor.execute(message, "default")

      # Latency should be ~1000ms, NOT ~7_201_000ms (which would indicate
      # the timezone offset was incorrectly applied)
      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(500, 5_000))
      )
    end
  end

  describe "Circuit breaker resume_at" do
    before do
      Pgbus::QueueState.delete_all if Pgbus::QueueState.table_exists?
    end

    it "auto-resumes correctly when resume_at was stored in non-UTC timezone" do
      # Skip if queue_states table doesn't exist
      skip "pgbus_queue_states table not available" unless Pgbus::QueueState.table_exists?

      breaker = Pgbus::CircuitBreaker.new(config: Pgbus.configuration)

      # Create a queue state that should have already resumed (resume_at in the past)
      Pgbus::QueueState.create!(
        queue_name: "tz-test-queue",
        paused: true,
        paused_reason: "circuit_breaker: test",
        paused_at: Time.current - 120,
        circuit_breaker_trip_count: 1,
        circuit_breaker_resume_at: Time.current - 60
      )

      # The breaker should auto-resume since resume_at is in the past
      paused = breaker.paused?("tz-test-queue")
      expect(paused).to be false
    end
  end
end
