# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Worker do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:executor) { instance_double(Pgbus::ActiveJob::Executor) }
  let(:pool) do
    instance_double(
      Pgbus::ExecutionPools::ThreadPool,
      capacity: 5, available_capacity: 5, idle?: true,
      shutdown: true, kill: true, wait_for_termination: true,
      metadata: { mode: :threads, capacity: 5, busy: 0 }
    )
  end
  let(:circuit_breaker) { instance_double(Pgbus::CircuitBreaker, paused?: false, record_success: nil, record_failure: nil) }
  let(:worker) { described_class.new(queues: %w[default], threads: 5) }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
    allow(Pgbus::CircuitBreaker).to receive(:new).and_return(circuit_breaker)
  end

  describe "#initialize" do
    it "stores config, queues, and creates an execution pool" do
      expect(worker.queues).to eq(%w[default])
      expect(worker.threads).to eq(5)
      expect(worker.config).to be_a(Pgbus::Configuration)
      expect(Pgbus::ExecutionPools).to have_received(:build).with(
        mode: :threads, capacity: 5, on_state_change: an_instance_of(Proc)
      )
    end

    it "defaults to threads execution mode" do
      expect(worker.execution_mode).to eq(:threads)
    end

    it "accepts async execution mode" do
      async_pool = instance_double(
        Pgbus::ExecutionPools::AsyncPool,
        capacity: 50, available_capacity: 50, idle?: true,
        shutdown: true, kill: true, wait_for_termination: true,
        metadata: { mode: :async, capacity: 50, busy: 0 }
      )
      allow(Pgbus::ExecutionPools).to receive(:build)
        .with(mode: :async, capacity: 50, on_state_change: an_instance_of(Proc))
        .and_return(async_pool)

      async_worker = described_class.new(queues: %w[default], threads: 50, execution_mode: :async)
      expect(async_worker.execution_mode).to eq(:async)
    end

    it "initializes stats tracking" do
      expect(worker.stats[:jobs_processed]).to eq(0)
      expect(worker.stats[:jobs_failed]).to eq(0)
      expect(worker.stats[:in_flight]).to eq(0)
      expect(worker.stats[:state]).to eq(:starting)
      expect(worker.stats[:execution_mode]).to eq(:threads)
      expect(worker.stats[:started_at]).to be_a(Time)
      expect(worker.stats[:mode]).to eq(:threads)
      expect(worker.stats[:capacity]).to eq(5)
    end

    it "raises ArgumentError for an invalid group_mode" do
      expect do
        described_class.new(queues: %w[default], threads: 5, group_mode: :nope)
      end.to raise_error(ArgumentError, /group_mode/)
    end
  end

  describe "#graceful_shutdown" do
    before { worker.instance_variable_get(:@lifecycle).transition_to!(:running) }
    after { Pgbus.stopping = false }

    it "transitions to draining state" do
      worker.graceful_shutdown
      expect(worker.instance_variable_get(:@lifecycle).state).to eq(:draining)
    end

    it "sets Pgbus.stopping for ActiveJob::Continuation support" do
      expect { worker.graceful_shutdown }.to change(Pgbus, :stopping).from(false).to(true)
    end
  end

  describe "#immediate_shutdown" do
    before { worker.instance_variable_get(:@lifecycle).transition_to!(:running) }
    after { Pgbus.stopping = false }

    it "transitions to stopped state and kills the pool" do
      worker.immediate_shutdown
      expect(worker.instance_variable_get(:@lifecycle).state).to eq(:stopped)
      expect(pool).to have_received(:kill)
    end

    it "sets Pgbus.stopping for ActiveJob::Continuation support" do
      expect { worker.immediate_shutdown }.to change(Pgbus, :stopping).from(false).to(true)
    end
  end

  describe "#recycle_needed? (private)" do
    it "returns false when no limits are configured" do
      expect(worker.send(:recycle_needed?)).to be false
    end

    context "when max_jobs_per_worker is exceeded" do
      before { worker.config.max_jobs_per_worker = 100 }

      it "returns true when jobs_processed reaches the limit" do
        worker.instance_variable_get(:@jobs_processed).value = 100
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        worker.instance_variable_get(:@jobs_processed).value = 50
        expect(worker.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { worker.config.max_worker_lifetime = 60 }

      it "returns true when lifetime is exceeded" do
        worker.instance_variable_set(:@started_at_monotonic, Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120)
        expect(worker.send(:recycle_needed?)).to be true
      end
    end
  end

  describe "#fetch_messages (private)" do
    context "with a single queue" do
      it "reads a batch from the single queue" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default priority], threads: 4) }

      it "uses read_multi to fetch from all queues in a single call, capping the total with limit:" do
        allow(mock_client).to receive(:read_multi).and_return([])
        worker.send(:fetch_messages, 4)
        expect(mock_client).to have_received(:read_multi).with(%w[default priority], qty: 4, limit: 4)
      end

      it "tags each message with its source queue from the queue_name field" do
        prefix = worker.config.queue_prefix
        msg1 = build_message_double(msg_id: 1, message: '{"a":1}')
        msg2 = build_message_double(msg_id: 2, message: '{"b":2}')
        allow(msg1).to receive(:queue_name).and_return("#{prefix}_default")
        allow(msg2).to receive(:queue_name).and_return("#{prefix}_priority")

        allow(mock_client).to receive(:read_multi).and_return([msg1, msg2])

        results = worker.send(:fetch_messages, 4)
        expect(results).to eq([["default", msg1], ["priority", msg2]])
      end
    end

    context "with three or more queues under backpressure (regression: issue #123)" do
      let(:worker) { described_class.new(queues: %w[default low statistics], threads: 5) }

      it "caps the total across queues at qty so the execution pool cannot overflow" do
        allow(mock_client).to receive(:read_multi).and_return([])
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_multi)
          .with(%w[default low statistics], qty: 5, limit: 5)
      end
    end

    context "when an error occurs" do
      before { allow(mock_client).to receive(:read_batch).and_raise(StandardError, "connection lost") }

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end
    end

    context "when a queue table is missing (deleted queue)" do
      let(:prefix) { worker.config.queue_prefix }
      let(:error_message) do
        "Database connection error: ERROR:  relation \"pgmq.q_#{prefix}_stale_queue\" does not exist"
      end

      before do
        worker.instance_variable_set(:@queues, %w[default stale_queue])
        allow(mock_client).to receive(:read_multi)
          .and_raise(StandardError, error_message)
      end

      it "evicts the deleted queue from the worker queue list" do
        worker.send(:fetch_messages, 5)
        expect(worker.queues).to eq(%w[default])
      end

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end
    end

    context "when eviction removes ALL queues (regression: issue #174)" do
      let(:prefix) { worker.config.queue_prefix }

      before do
        worker.instance_variable_set(:@queues, %w[only_queue])
        error_message = "Database connection error: ERROR:  relation \"pgmq.q_#{prefix}_only_queue\" does not exist"
        allow(mock_client).to receive(:read_batch)
          .and_raise(StandardError, error_message)
      end

      it "recovers by falling back to the default queue" do
        worker.send(:fetch_messages, 5)
        expect(worker.queues).not_to be_empty
        expect(worker.queues).to eq([worker.config.default_queue])
      end
    end

    context "when circuit breaker pauses all queues (#174 diagnostic)" do
      let(:worker) { described_class.new(queues: %w[default low], threads: 5) }
      let(:warn_messages) { [] }

      before do
        allow(circuit_breaker).to receive(:paused?).and_return(true)
        allow(Pgbus.logger).to receive(:debug) do |&block|
          warn_messages << block.call if block
        end
      end

      it "logs which queues were filtered out" do
        worker.send(:fetch_messages, 5)
        filtered_log = warn_messages.find { |m| m.include?("all queues filtered") }
        expect(filtered_log).not_to be_nil
      end
    end

    context "with group_mode: :fifo" do
      let(:worker) { described_class.new(queues: %w[default], threads: 5, group_mode: :fifo) }
      let(:messages) { [build_message_double(msg_id: 1), build_message_double(msg_id: 2)] }

      before do
        allow(mock_client).to receive(:read_grouped).and_return(messages)
      end

      it "uses read_grouped instead of read_batch" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_grouped).with("default", qty: 5)
        expect(mock_client).not_to have_received(:read_batch)
      end

      it "returns [queue_name, message] pairs" do
        results = worker.send(:fetch_messages, 5)
        expect(results).to eq([["default", messages[0]], ["default", messages[1]]])
      end
    end

    context "with group_mode: :round_robin" do
      let(:worker) { described_class.new(queues: %w[default], threads: 5, group_mode: :round_robin) }
      let(:messages) { [build_message_double(msg_id: 1)] }

      before do
        allow(mock_client).to receive(:read_grouped_rr).and_return(messages)
      end

      it "uses read_grouped_rr instead of read_batch" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_grouped_rr).with("default", qty: 5)
        expect(mock_client).not_to have_received(:read_batch)
      end
    end

    context "with group_mode: :fifo and multiple queues" do
      let(:worker) { described_class.new(queues: %w[critical default], threads: 5, group_mode: :fifo) }
      let(:critical_msgs) { [build_message_double(msg_id: 1)] }
      let(:default_msgs) { [build_message_double(msg_id: 2)] }

      before do
        allow(mock_client).to receive(:read_grouped) do |queue, **_kwargs|
          case queue
          when "critical" then critical_msgs
          when "default" then default_msgs
          else []
          end
        end
      end

      it "reads from each queue with grouped reads, respecting qty limit" do
        results = worker.send(:fetch_messages, 5)
        expect(results.size).to eq(2)
        expect(mock_client).to have_received(:read_grouped).with("critical", qty: 5)
        expect(mock_client).to have_received(:read_grouped).with("default", qty: 4)
      end
    end
  end

  describe "prefetch flow control" do
    let(:wake_signal) { worker.instance_variable_get(:@wake_signal) }

    before { allow(wake_signal).to receive(:wait) }

    context "when prefetch_limit is nil (default)" do
      before { worker.config.prefetch_limit = nil }

      it "does not cap fetch quantity" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "when prefetch_limit is configured" do
      before { worker.config.prefetch_limit = 3 }
      after { worker.config.prefetch_limit = nil }

      it "caps fetch to prefetch_limit when below idle threads" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 3)
      end

      it "waits when in_flight >= prefetch_limit" do
        worker.instance_variable_get(:@in_flight).value = 3
        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch)
        expect(wake_signal).to have_received(:wait)
      end

      it "uses min of idle and available prefetch" do
        # 2 in flight, limit 3 => available = 1, idle = 5 => fetch 1
        worker.instance_variable_get(:@in_flight).value = 2
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 1)
      end
    end

    it "increments in_flight when messages are fetched" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:claim_and_execute)
      # After process_message completes (via yield), in_flight should be back to 0
      expect(worker.stats[:in_flight]).to eq(0)
    end

    it "decrements in_flight even when executor raises" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:claim_and_execute)
      expect(worker.stats[:in_flight]).to eq(0)
    end
  end

  describe "#process_message (private)" do
    let(:message) { build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}') }

    it "executes the message and increments jobs_processed" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(executor).to have_received(:execute).with(message, "default", source_queue: nil)
      expect(worker.stats[:jobs_processed]).to eq(1)
    end

    it "increments jobs_failed when executor returns :failed" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_processed]).to eq(1)
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "increments jobs_failed when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "signals circuit breaker success on successful execution" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_success).with("default")
    end

    it "signals circuit breaker failure on failed execution" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end

    it "signals circuit breaker failure when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end
  end

  describe "circuit breaker integration" do
    before { allow(worker.instance_variable_get(:@wake_signal)).to receive(:wait) }

    it "skips paused queues" do
      allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
      allow(mock_client).to receive(:read_batch).and_return([])

      worker.send(:claim_and_execute)
      expect(mock_client).not_to have_received(:read_batch)
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default events], threads: 4) }

      it "only reads from non-paused queues" do
        allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
        allow(circuit_breaker).to receive(:paused?).with("events").and_return(false)
        allow(mock_client).to receive(:read_batch).and_return([])

        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch).with("default", anything)
        # With only one non-paused queue, falls back to single-queue read_batch
        expect(mock_client).to have_received(:read_batch).with("events", qty: 5)
      end

      it "uses read_multi when multiple queues are active" do
        allow(circuit_breaker).to receive(:paused?).and_return(false)
        allow(mock_client).to receive(:read_multi).and_return([])

        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_multi).with(%w[default events], qty: 5, limit: 5)
      end
    end
  end

  describe "single active consumer" do
    let(:queue_lock) { instance_double(Pgbus::Process::QueueLock, unlock_all: nil, held_queues: []) }
    let(:worker) { described_class.new(queues: %w[default events], threads: 4, single_active_consumer: true) }

    before do
      allow(Pgbus::Process::QueueLock).to receive(:new).and_return(queue_lock)
      allow(worker.instance_variable_get(:@wake_signal)).to receive(:wait)
    end

    it "only reads from queues where advisory lock is held" do
      allow(queue_lock).to receive(:try_lock).with("default").and_return(true)
      allow(queue_lock).to receive(:try_lock).with("events").and_return(false)
      allow(mock_client).to receive(:read_batch).and_return([])

      worker.send(:claim_and_execute)
      expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      expect(mock_client).not_to have_received(:read_batch).with("events", anything)
    end

    it "skips all queues when no locks acquired" do
      allow(queue_lock).to receive(:try_lock).and_return(false)
      allow(mock_client).to receive(:read_batch)

      worker.send(:claim_and_execute)
      expect(mock_client).not_to have_received(:read_batch)
    end

    it "exposes single_active_consumer in stats" do
      expect(worker.stats[:single_active_consumer]).to be true
    end
  end

  describe "#refresh_wildcard_queues (private)" do
    let(:worker) { described_class.new(queues: %w[*], threads: 5) }
    let(:prefix) { worker.config.queue_prefix }
    let(:conn) { double("connection") }

    before do
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    end

    it "is a no-op for non-wildcard workers" do
      static_worker = described_class.new(queues: %w[default], threads: 5)
      static_worker.send(:refresh_wildcard_queues)
      expect(static_worker.queues).to eq(%w[default])
    end

    it "re-resolves when enough time has passed" do
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default", "#{prefix}_events"])
      worker.send(:resolve_wildcard_queues)
      expect(worker.queues).to eq(%w[default events])

      # Simulate time passing and queues changing
      worker.instance_variable_set(:@last_wildcard_resolve, Process.clock_gettime(Process::CLOCK_MONOTONIC) - 60)
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default"])
      worker.send(:refresh_wildcard_queues)
      expect(worker.queues).to eq(%w[default])
    end

    it "skips re-resolve when interval has not elapsed" do
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default"])
      worker.send(:resolve_wildcard_queues)

      # Try to refresh immediately — should be a no-op
      worker.send(:refresh_wildcard_queues)
      # Only called once (the initial resolve)
      expect(conn).to have_received(:select_values).once
    end
  end

  describe "zombie message detection" do
    let(:wake_signal) { worker.instance_variable_get(:@wake_signal) }

    before do
      allow(wake_signal).to receive(:wait)
      allow(Pgbus::FailedEventRecorder).to receive(:exists?).and_return(false)
    end

    context "when zombie_detection is enabled (default)" do
      let(:warn_messages) { [] }

      before do
        allow(Pgbus.logger).to receive(:warn) do |&block|
          warn_messages << block.call if block
        end
      end

      it "logs a warning for redelivered messages with no prior failure" do
        msg = build_message_double(msg_id: 99, message: '{"job_class":"TestJob"}', read_ct: 2)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(Pgbus::FailedEventRecorder).to have_received(:exists?)
          .with(queue_name: "default", msg_id: 99)
        zombie_log = warn_messages.find { |m| m.include?("Zombie message redelivered") }
        expect(zombie_log).to include("msg_id=99")
        expect(zombie_log).to include("read_ct=2")
      end

      it "does not warn for first-time messages (read_ct=1)" do
        msg = build_message_double(msg_id: 100, message: '{"job_class":"TestJob"}', read_ct: 1)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(warn_messages).not_to include(a_string_matching(/Zombie/))
      end

      it "does not warn when a prior failure is recorded" do
        msg = build_message_double(msg_id: 101, message: '{"job_class":"TestJob"}', read_ct: 2)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)
        allow(Pgbus::FailedEventRecorder).to receive(:exists?).and_return(true)

        worker.send(:claim_and_execute)

        expect(warn_messages).not_to include(a_string_matching(/Zombie/))
      end
    end

    context "when zombie_detection is disabled" do
      before { worker.config.zombie_detection = false }
      after { worker.config.zombie_detection = true }

      it "does not check for zombie messages" do
        msg = build_message_double(msg_id: 102, message: '{"job_class":"TestJob"}', read_ct: 3)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(Pgbus::FailedEventRecorder).not_to have_received(:exists?)
      end
    end
  end

  describe "consumer priority" do
    it "defaults to 0" do
      expect(worker.stats[:consumer_priority]).to eq(0)
    end

    it "accepts a custom priority" do
      priority_worker = described_class.new(queues: %w[default], threads: 5, consumer_priority: 10)
      expect(priority_worker.stats[:consumer_priority]).to eq(10)
    end
  end

  describe "NOTIFY-gated wakeup wiring" do
    # These specs pin the load-bearing private API around the NotifyListener:
    # construction, queue sync, channel ↔ physical-queue conversion, the
    # raised wait_timeout when the listener is active, and shutdown cleanup.
    # The listener itself is stubbed — its internals are covered in
    # notify_listener_spec.rb; here we only exercise how the worker wires it.

    let(:fake_listener) do
      instance_double(
        Pgbus::Process::NotifyListener,
        start: nil, stop: nil, add_queue: nil, remove_queue: nil,
        listening_to: []
      ).tap { |l| allow(l).to receive(:start).and_return(l) }
    end

    describe "#notify_wakeup?" do
      it "is true when config.worker_notify_wakeup? is true" do
        allow(worker.config).to receive(:worker_notify_wakeup?).and_return(true)
        expect(worker.send(:notify_wakeup?)).to be true
      end

      it "is false when config.worker_notify_wakeup? is false" do
        allow(worker.config).to receive(:worker_notify_wakeup?).and_return(false)
        expect(worker.send(:notify_wakeup?)).to be false
      end

      it "is false when config does not respond to worker_notify_wakeup? (old configs)" do
        stripped_config = double("Config")
        allow(stripped_config).to receive(:respond_to?).with(:worker_notify_wakeup?).and_return(false)
        worker.instance_variable_set(:@config, stripped_config)
        expect(worker.send(:notify_wakeup?)).to be false
      end
    end

    describe "#physical_queue_names" do
      let(:prefix) { worker.config.queue_prefix }

      it "prefixes each logical queue with the configured queue_prefix" do
        multi_worker = described_class.new(queues: %w[default low], threads: 4)
        expect(multi_worker.send(:physical_queue_names))
          .to eq(["#{prefix}_default", "#{prefix}_low"])
      end

      it "uses the current @queues, not the initial set (post-eviction safe)" do
        worker.instance_variable_set(:@queues, %w[events])
        expect(worker.send(:physical_queue_names)).to eq(["#{prefix}_events"])
      end
    end

    describe "#channel_to_physical" do
      it "round-trips a channel name back to its physical queue" do
        prefix = worker.config.queue_prefix
        channel = "#{Pgbus::Process::NotifyListener::CHANNEL_PREFIX}#{prefix}_default" \
                  "#{Pgbus::Process::NotifyListener::CHANNEL_SUFFIX}"
        expect(worker.send(:channel_to_physical, channel)).to eq("#{prefix}_default")
      end
    end

    describe "#wake_timeout" do
      it "returns effective_polling_interval when notify_wakeup? is off" do
        allow(worker).to receive(:notify_wakeup?).and_return(false)
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end

      it "returns effective_polling_interval when wakeup is on but no listener attached yet" do
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        worker.instance_variable_set(:@notify_listener, nil)
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end

      it "raises the wait to the NOTIFY_FALLBACK_POLL_SECONDS ceiling when a listener is active" do
        # When NOTIFY drives latency, the empty-fetch wait is a safety net,
        # not the steady-state cadence. The ceiling lets one missed NOTIFY
        # cost bounded latency, never a stuck queue.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        worker.instance_variable_set(:@notify_listener, fake_listener)
        expect(worker.send(:wake_timeout))
          .to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
      end
    end

    describe "#start_notify_listener" do
      it "does not construct a listener when notify_wakeup? is off" do
        allow(worker).to receive(:notify_wakeup?).and_return(false)
        allow(Pgbus::Process::NotifyListener).to receive(:new)
        worker.send(:start_notify_listener)
        expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
        expect(worker.instance_variable_get(:@notify_listener)).to be_nil
      end

      it "constructs and starts a listener for every physical queue when wakeup is on" do
        prefix = worker.config.queue_prefix
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new).with(
          physical_queues: ["#{prefix}_default"],
          on_wake: an_instance_of(Proc),
          connection_options: { dbname: "test" },
          health_check_ms: an_instance_of(Integer),
          logger: Pgbus.logger
        )
        expect(fake_listener).to have_received(:start)
        expect(worker.instance_variable_get(:@notify_listener)).to be(fake_listener)
      end

      it "clamps health_check_ms below 250ms up to the floor" do
        # polling_interval 0.1s → 100ms → clamped up to 250ms minimum.
        worker.config.polling_interval = 0.1
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new)
          .with(hash_including(health_check_ms: 250))
      end

      it "clamps health_check_ms above 5000ms down to the ceiling" do
        # polling_interval 30s → 30000ms → clamped down to 5000ms maximum.
        # Build a fresh worker since polling_interval mutation must not
        # leak into other examples.
        big_poll_worker = described_class.new(queues: %w[default], threads: 5)
        big_poll_worker.config.polling_interval = 30
        allow(big_poll_worker).to receive(:notify_wakeup?).and_return(true)
        allow(big_poll_worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        big_poll_worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new)
          .with(hash_including(health_check_ms: 5000))
      end

      it "swallows listener startup errors and falls back to polling without crashing the worker" do
        # A NotifyListener failure must NEVER take down the worker — that
        # was the whole point of making it default-on without a feature
        # flag at the worker layer. The rescue logs and leaves
        # @notify_listener nil; wake_timeout reverts to the polling
        # interval as if NOTIFY had never been enabled.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_raise(StandardError, "no direct port configured")
        allow(Pgbus.logger).to receive(:error)

        expect { worker.send(:start_notify_listener) }.not_to raise_error
        expect(worker.instance_variable_get(:@notify_listener)).to be_nil
        expect(Pgbus.logger).to have_received(:error)
      end
    end

    describe "#sync_notify_listener_queues" do
      let(:multi_worker) { described_class.new(queues: %w[default low], threads: 4) }
      let(:queue_prefix) { multi_worker.config.queue_prefix }

      before { multi_worker.instance_variable_set(:@notify_listener, fake_listener) }

      it "adds new physical queues and removes dropped ones based on the set difference" do
        # Listener currently watches default + statistics; @queues is
        # default + low. Expect statistics to be unlistened and low to
        # be added; default is untouched because it overlaps both sets.
        ch_prefix = Pgbus::Process::NotifyListener::CHANNEL_PREFIX
        ch_suffix = Pgbus::Process::NotifyListener::CHANNEL_SUFFIX
        allow(fake_listener).to receive(:listening_to).and_return(
          ["#{ch_prefix}#{queue_prefix}_default#{ch_suffix}",
           "#{ch_prefix}#{queue_prefix}_statistics#{ch_suffix}"]
        )

        multi_worker.send(:sync_notify_listener_queues)

        expect(fake_listener).to have_received(:add_queue).with("#{queue_prefix}_low")
        expect(fake_listener).to have_received(:remove_queue).with("#{queue_prefix}_statistics")
        expect(fake_listener).not_to have_received(:add_queue).with("#{queue_prefix}_default")
      end

      it "is a no-op when no listener is attached" do
        multi_worker.instance_variable_set(:@notify_listener, nil)
        expect { multi_worker.send(:sync_notify_listener_queues) }.not_to raise_error
        expect(fake_listener).not_to have_received(:add_queue)
        expect(fake_listener).not_to have_received(:remove_queue)
      end

      it "logs and survives a listener error so a sync failure doesn't crash the loop" do
        allow(fake_listener).to receive(:listening_to).and_raise(StandardError, "listener gone")
        allow(Pgbus.logger).to receive(:warn)

        expect { multi_worker.send(:sync_notify_listener_queues) }.not_to raise_error
        expect(Pgbus.logger).to have_received(:warn)
      end
    end

    describe "#shutdown" do
      it "stops the notify listener before draining the pool" do
        worker.instance_variable_set(:@notify_listener, fake_listener)
        worker.send(:shutdown)
        expect(fake_listener).to have_received(:stop)
      end

      it "is a no-op for the listener when none was started (default-off path)" do
        worker.instance_variable_set(:@notify_listener, nil)
        expect { worker.send(:shutdown) }.not_to raise_error
      end
    end
  end
end
