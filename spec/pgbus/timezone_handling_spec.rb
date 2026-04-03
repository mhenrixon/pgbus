# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

# Reproduces timezone-related delays when Rails is configured with a non-UTC
# timezone (e.g. Africa/Casablanca). The root cause is inconsistent use of
# Time.now.utc vs Time.current across the codebase: when
# config.active_record.default_timezone = :local, AR interprets stored
# timestamps in the system's local time, so writing Time.now.utc and reading
# back through AR produces values offset by the timezone difference.
RSpec.describe "Timezone handling" do
  around do |example|
    original_tz = Time.zone
    Time.zone = "Africa/Casablanca"
    example.run
  ensure
    Time.zone = original_tz
  end

  describe "Adapter#enqueue_at delay calculation" do
    subject(:adapter) { Pgbus::ActiveJob::Adapter.new }

    let(:mock_client) { build_mock_client }
    let(:job_id) { SecureRandom.uuid }
    let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
    let(:serialized_hash) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
      allow(mock_client).to receive(:send_message).and_return(1)
    end

    it "calculates correct delay regardless of Time.zone setting" do
      # Simulate ActiveJob passing a UTC epoch timestamp 60 seconds from now
      future_timestamp = Time.current.to_f + 60

      adapter.enqueue_at(job, future_timestamp)

      expect(mock_client).to have_received(:send_message).with(
        "default", serialized_hash, delay: a_value_between(59, 61), priority: nil
      )
    end

    it "uses Time.current.to_f (not Time.now.to_f) to compute delay" do
      # When system TZ differs from Rails TZ, Time.now.to_f and Time.current.to_f
      # both return UTC epoch, but using Time.current is the Rails-idiomatic way
      # and avoids confusion. The delay must always be correct regardless of timezone.
      future_timestamp = Time.current.to_f + 120

      adapter.enqueue_at(job, future_timestamp)

      expect(mock_client).to have_received(:send_message).with(
        "default", serialized_hash, delay: a_value_between(119, 121), priority: nil
      )
    end
  end

  describe "Adapter#enqueue_all scheduled_at comparison" do
    subject(:adapter) { Pgbus::ActiveJob::Adapter.new }

    let(:mock_client) { build_mock_client }
    let(:job_id) { SecureRandom.uuid }
    let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
    let(:serialized_hash) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
      allow(mock_client).to receive_messages(send_message: 1, send_batch: [1])
    end

    it "correctly identifies future jobs using Time.current" do
      # A job scheduled 60 seconds from now in the app's timezone
      future_time = Time.current + 60
      allow(job).to receive(:scheduled_at).and_return(future_time)

      adapter.enqueue_all([job])

      # Should be enqueued with a delay (via enqueue_at), not immediately
      expect(mock_client).to have_received(:send_message).with(
        "default", serialized_hash, delay: a_value_between(59, 61), priority: nil
      )
    end
  end

  describe "Executor#compute_enqueue_latency" do
    subject(:executor) { Pgbus::ActiveJob::Executor.new(client: mock_client, config: config) }

    let(:mock_client) { build_mock_client }
    let(:config) { Pgbus.configuration }
    let(:job_id) { SecureRandom.uuid }
    let(:job_payload) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end
    let(:message_json) { JSON.generate(job_payload) }
    let(:job_double) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }

    before do
      allow(ActiveJob::Base).to receive(:deserialize).and_return(job_double)
      allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
      stub_const("Pgbus::JobStat", Class.new) unless defined?(Pgbus::JobStat)
      allow(Pgbus::JobStat).to receive(:record!)
    end

    it "computes correct latency when enqueued_at has timezone offset" do
      # PGMQ returns enqueued_at as TIMESTAMPTZ which includes offset
      enqueued_at = (Time.current - 1).utc.iso8601(6)
      message = build_message_double(msg_id: 1, message: message_json, read_ct: 1, enqueued_at: enqueued_at)

      executor.execute(message, "default")

      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(900, 1500))
      )
    end

    it "computes correct latency when enqueued_at lacks timezone info" do
      # Edge case: timestamp string without TZ offset. Should still compute
      # correctly by assuming UTC, not local time.
      one_second_ago_utc = (Time.now.utc - 1).strftime("%Y-%m-%d %H:%M:%S.%6N")
      message = build_message_double(msg_id: 2, message: message_json, read_ct: 1, enqueued_at: one_second_ago_utc)

      executor.execute(message, "default")

      # Latency should be ~1000ms, NOT offset by timezone hours
      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(800, 2000))
      )
    end
  end

  describe "Concurrency::BlockedExecution.resolve_delay" do
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus::BlockedExecution).to receive(:transaction).and_yield
    end

    it "computes correct delay from scheduled_at in non-UTC timezone" do
      # scheduled_at stored as UTC ISO8601 string with offset
      future_utc = (Time.current + 120).utc.iso8601
      released = {
        queue_name: "default",
        payload: { "job_class" => "TestJob", "scheduled_at" => future_utc }
      }
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(1)

      Pgbus::Concurrency::BlockedExecution.promote_next("TestJob-42", client: mock_client)

      # Delay should be ~120 seconds, not offset by timezone
      expect(mock_client).to have_received(:send_message).with(
        "default",
        hash_including("job_class" => "TestJob"),
        delay: a_value_between(118, 122)
      )
    end
  end

  describe "Concurrency::BlockedExecution.insert timezone consistency" do
    it "sets expires_at using Time.current (not Time.now.utc)" do
      allow(Pgbus::BlockedExecution).to receive(:create!)

      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "k", queue_name: "q",
        payload: { "job_class" => "T" }, duration: 900
      )

      expect(Pgbus::BlockedExecution).to have_received(:create!).with(
        hash_including(:expires_at)
      ) do |args|
        expires_at = args[:expires_at]
        expected = Time.current + 900
        # Should be within 2 seconds of Time.current + duration
        expect(expires_at).to be_within(2).of(expected)
      end
    end
  end

  describe "Concurrency::Semaphore.acquire timezone consistency" do
    it "sets expires_at using Time.current (not Time.now.utc)" do
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:acquired)

      Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 900)

      expect(Pgbus::Semaphore).to have_received(:acquire!) do |_key, _max, expires_at|
        expected = Time.current + 900
        expect(expires_at).to be_within(2).of(expected)
      end
    end
  end

  describe "Concurrency::Semaphore.expire_stale timezone consistency" do
    it "uses Time.current for expiration comparison" do
      mock_result = double("result", rows: [])
      mock_conn = double("connection")
      allow(Pgbus::Semaphore).to receive(:connection).and_return(mock_conn)
      allow(mock_conn).to receive(:exec_query).and_return(mock_result)

      Pgbus::Concurrency::Semaphore.expire_stale

      expect(mock_conn).to have_received(:exec_query).with(
        anything, anything, [a_value_within(2).of(Time.current)]
      )
    end
  end

  describe "CircuitBreaker timezone consistency" do
    subject(:breaker) { Pgbus::CircuitBreaker.new(config: config) }

    let(:config) { Pgbus.configuration }

    it "uses consistent time source for cache TTL checks" do
      allow(Pgbus::QueueState).to receive(:find_by).and_return(nil)

      # First call populates cache
      breaker.paused?("test_queue")

      # Advance time slightly
      allow(Time).to receive(:current).and_return(Time.current + 5)

      # Second call should use cache (within TTL)
      breaker.paused?("test_queue")

      # QueueState.find_by should only be called once (second call uses cache)
      expect(Pgbus::QueueState).to have_received(:find_by).once
    end
  end

  describe "Recurring::Schedule timezone awareness" do
    it "evaluates cron schedules using Time.current (app timezone)" do
      schedule = Pgbus::Recurring::Schedule.new(config: Pgbus.configuration)

      # due_tasks should accept and work correctly with Time.current
      # (this verifies it doesn't break when given a TimeWithZone)
      expect { schedule.due_tasks(Time.current) }.not_to raise_error
    end
  end
end
