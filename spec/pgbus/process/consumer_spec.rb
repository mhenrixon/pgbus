# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Pgbus::Process::Consumer do
  let(:mock_client) { build_mock_client }
  let(:mock_pool) do
    instance_double(
      Pgbus::ExecutionPools::ThreadPool,
      capacity: 3, available_capacity: 3, idle?: true,
      kill: nil, shutdown: nil, wait_for_termination: nil,
      metadata: { mode: :threads, capacity: 3, busy: 0 }
    )
  end
  # NotifyListener#start returns self so callers can chain `.new(...).start`.
  let(:fake_listener) do
    instance_double(
      Pgbus::Process::NotifyListener,
      stop: nil, running?: true,
      listening_to: [], add_queue: nil, remove_queue: nil
    ).tap { |l| allow(l).to receive(:start).and_return(l) }
  end
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:subscriber_a) { instance_double(Pgbus::EventBus::Subscriber, pattern: "orders.#", queue_name: "q_orders") }
  let(:subscriber_b) { instance_double(Pgbus::EventBus::Subscriber, pattern: "payments.completed", queue_name: "q_payments") }
  let(:subscriber_c) { instance_double(Pgbus::EventBus::Subscriber, pattern: "shipping.label", queue_name: "q_shipping") }
  let(:registry) { instance_double(Pgbus::EventBus::Registry, subscribers: [subscriber_a, subscriber_b, subscriber_c]) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(mock_pool)
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
  end

  describe "#initialize" do
    it "stores topics, threads, and config" do
      consumer = described_class.new(topics: ["orders.#"], threads: 5)

      expect(consumer.topics).to eq(["orders.#"])
      expect(consumer.threads).to eq(5)
      expect(consumer.config).to eq(Pgbus.configuration)
    end

    it "wraps a single topic into an array" do
      consumer = described_class.new(topics: "orders.#")

      expect(consumer.topics).to eq(["orders.#"])
    end

    it "defaults threads to 3" do
      consumer = described_class.new(topics: ["orders.#"])

      expect(consumer.threads).to eq(3)
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      consumer = described_class.new(topics: ["orders.#"])
      consumer.graceful_shutdown

      expect(consumer.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down flag and kills the thread pool" do
      consumer = described_class.new(topics: ["orders.#"])
      consumer.immediate_shutdown

      expect(consumer.instance_variable_get(:@shutting_down)).to be true
      expect(mock_pool).to have_received(:kill)
    end
  end

  describe "setup_subscriptions (private)" do
    it "filters registry by topic overlap and collects unique queue names" do
      consumer = described_class.new(topics: ["payments.completed"])
      consumer.send(:setup_subscriptions)

      queue_names = consumer.instance_variable_get(:@queue_names)
      expect(queue_names).to include("q_payments")
      expect(queue_names).not_to include("q_shipping")
    end
  end

  describe "handle_message (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }
    let(:handler_instance) { double("handler", process: nil) }
    let(:handler_class) { double("HandlerClass", new: handler_instance) }
    let(:matching_subscriber) { instance_double(Pgbus::EventBus::Subscriber, handler_class: handler_class) }
    let(:message_body) { JSON.generate("headers" => { "routing_key" => "orders.created" }, "data" => { "id" => 42 }) }
    let(:message) { build_message_double(msg_id: 7, message: message_body) }

    before do
      allow(registry).to receive(:handlers_for).with("orders.created").and_return([matching_subscriber])
      consumer.instance_variable_set(:@queue_names, ["q_orders"])
    end

    it "parses routing_key, finds handlers, processes, and archives" do
      consumer.send(:handle_message, message, "q_orders")

      expect(registry).to have_received(:handlers_for).with("orders.created")
      expect(handler_instance).to have_received(:process).with(message)
      expect(mock_client).to have_received(:archive_message).with("q_orders", 7)
    end

    it "rescues errors gracefully and logs them" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))

      expect { consumer.send(:handle_message, message, "q_orders") }.not_to raise_error
    end

    context "when routing_key is in the message body (not headers)" do
      let(:message_body) { JSON.generate("routing_key" => "orders.shipped", "payload" => { "id" => 99 }) }
      let(:message) { build_message_double(msg_id: 8, message: message_body) }

      before do
        allow(registry).to receive(:handlers_for).with("orders.shipped").and_return([matching_subscriber])
      end

      it "extracts routing_key from the top-level body field" do
        consumer.send(:handle_message, message, "q_orders")

        expect(registry).to have_received(:handlers_for).with("orders.shipped")
        expect(handler_instance).to have_received(:process).with(message)
        expect(mock_client).to have_received(:archive_message).with("q_orders", 8)
      end
    end
  end

  describe "fetch_multi_consumer (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    before do
      consumer.instance_variable_set(:@queue_names, %w[q_orders q_payments q_shipping])
    end

    it "caps the total across queues at qty so the execution pool cannot overflow (issue #123)" do
      allow(mock_client).to receive(:read_multi).and_return([])
      consumer.send(:fetch_multi_consumer, 3)
      expect(mock_client).to have_received(:read_multi)
        .with(%w[q_orders q_payments q_shipping], qty: 3, limit: 3)
    end
  end

  describe "pattern_overlaps? (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "returns true for exact match" do
      expect(consumer.send(:pattern_overlaps?, "orders.created", "orders.created")).to be true
    end

    it "returns true when topic filter ends with #" do
      expect(consumer.send(:pattern_overlaps?, "orders.#", "orders.created")).to be true
    end

    it "returns true when subscription starts with topic prefix" do
      expect(consumer.send(:pattern_overlaps?, "orders.created", "orders.created.v2")).to be true
    end

    it "returns false for unrelated patterns" do
      expect(consumer.send(:pattern_overlaps?, "payments.completed", "shipping.label")).to be false
    end
  end

  describe "#recycle_needed? (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "returns false when no limits are configured" do
      expect(consumer.send(:recycle_needed?)).to be false
    end

    context "when max_jobs_per_worker is exceeded" do
      before { consumer.config.max_jobs_per_worker = 100 }
      after { consumer.config.max_jobs_per_worker = nil }

      it "returns true when jobs_processed reaches the limit" do
        consumer.instance_variable_get(:@jobs_processed).value = 100
        expect(consumer.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        consumer.instance_variable_get(:@jobs_processed).value = 50
        expect(consumer.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { consumer.config.max_worker_lifetime = 60 }
      after { consumer.config.max_worker_lifetime = nil }

      it "returns true when lifetime is exceeded" do
        consumer.instance_variable_set(:@started_at_monotonic, Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120)
        expect(consumer.send(:recycle_needed?)).to be true
      end
    end

    context "when max_memory_mb is exceeded" do
      before do
        consumer.config.max_memory_mb = 256
        Pgbus::Process::MemoryUsage.reset!
      end

      after do
        consumer.config.max_memory_mb = nil
        Pgbus::Process::MemoryUsage.reset!
      end

      it "returns true when memory exceeds the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(512)
        expect(consumer.send(:recycle_needed?)).to be true
      end

      it "returns false when memory is within the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(128)
        expect(consumer.send(:recycle_needed?)).to be false
      end
    end
  end

  describe "#check_recycle (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }
    let(:wake_signal) { consumer.instance_variable_get(:@wake_signal) }

    before { allow(wake_signal).to receive(:notify!) }

    it "does nothing when no recycle is needed" do
      consumer.send(:check_recycle)
      expect(consumer.instance_variable_get(:@shutting_down)).to be false
    end

    context "when a limit is exceeded" do
      before { consumer.config.max_jobs_per_worker = 10 }
      after { consumer.config.max_jobs_per_worker = nil }

      it "sets @shutting_down so the loop exits cleanly" do
        consumer.instance_variable_get(:@jobs_processed).value = 10
        consumer.send(:check_recycle)
        expect(consumer.instance_variable_get(:@shutting_down)).to be true
      end

      it "instruments pgbus.consumer.recycle with the reason" do
        consumer.instance_variable_get(:@jobs_processed).value = 10
        allow(Pgbus::Instrumentation).to receive(:instrument)
        consumer.send(:check_recycle)
        expect(Pgbus::Instrumentation).to have_received(:instrument)
          .with("pgbus.consumer.recycle", hash_including(reason: :max_jobs))
      end

      it "wakes the idle wait so the loop notices the shutdown immediately" do
        consumer.instance_variable_get(:@jobs_processed).value = 10
        consumer.send(:check_recycle)
        expect(wake_signal).to have_received(:notify!)
      end

      it "recycles only once even if called repeatedly" do
        consumer.instance_variable_get(:@jobs_processed).value = 10
        allow(Pgbus::Instrumentation).to receive(:instrument)
        consumer.send(:check_recycle)
        consumer.send(:check_recycle)
        expect(Pgbus::Instrumentation).to have_received(:instrument).once
      end
    end
  end

  describe "recycle counting in handle_message" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }
    let(:handler_instance) { double("handler", process: nil) }
    let(:handler_class) { double("HandlerClass", new: handler_instance) }
    let(:matching_subscriber) { instance_double(Pgbus::EventBus::Subscriber, handler_class: handler_class) }
    let(:message_body) { JSON.generate("headers" => { "routing_key" => "orders.created" }) }
    let(:message) { build_message_double(msg_id: 7, message: message_body) }

    before do
      allow(registry).to receive(:handlers_for).with("orders.created").and_return([matching_subscriber])
      consumer.instance_variable_set(:@queue_names, ["q_orders"])
    end

    it "increments @jobs_processed after a successful handle" do
      expect { consumer.send(:handle_message, message, "q_orders") }
        .to change { consumer.instance_variable_get(:@jobs_processed).value }.by(1)
    end

    it "increments @jobs_processed when a handler raises (so recycling still counts failures)" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))

      expect { consumer.send(:handle_message, message, "q_orders") }
        .to change { consumer.instance_variable_get(:@jobs_processed).value }.by(1)
    end

    it "increments @jobs_processed when a message is routed to the DLQ (poison queue still recycles)" do
      dlq_message = build_message_double(msg_id: 9, message: message_body, read_ct: 99)
      allow(consumer.config).to receive(:max_retries).and_return(5)

      expect { consumer.send(:handle_message, dlq_message, "q_orders") }
        .to change { consumer.instance_variable_get(:@jobs_processed).value }.by(1)
      expect(mock_client).to have_received(:move_to_dead_letter).with("q_orders", dlq_message)
    end
  end

  describe "#wake_timeout (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "returns polling_interval when notify_wakeup? is off" do
      allow(consumer).to receive(:notify_wakeup?).and_return(false)
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end

    it "returns polling_interval when wakeup is on but no listener attached yet" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      consumer.instance_variable_set(:@notify_listener, nil)
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end

    it "raises the wait to NOTIFY_FALLBACK_POLL_SECONDS when a listener is active" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      consumer.instance_variable_set(:@notify_listener, fake_listener)
      expect(consumer.send(:wake_timeout))
        .to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
    end

    it "returns polling_interval when the listener thread has died" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(fake_listener).to receive(:running?).and_return(false)
      consumer.instance_variable_set(:@notify_listener, fake_listener)
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end
  end

  describe "#start_notify_listener (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    before { consumer.instance_variable_set(:@queue_names, ["orders"]) }

    it "does not construct a listener when notify_wakeup? is off" do
      allow(consumer).to receive(:notify_wakeup?).and_return(false)
      allow(Pgbus::Process::NotifyListener).to receive(:new)
      consumer.send(:start_notify_listener)
      expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
      expect(consumer.instance_variable_get(:@notify_listener)).to be_nil
    end

    it "constructs and starts a listener over the physical queue names when wakeup is on" do
      prefix = consumer.config.queue_prefix
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(consumer.config).to receive(:worker_notify_connection_options).and_return({ dbname: "test" })
      allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

      consumer.send(:start_notify_listener)

      expect(Pgbus::Process::NotifyListener).to have_received(:new).with(
        physical_queues: ["#{prefix}_orders"],
        on_wake: an_instance_of(Proc),
        connection_options: { dbname: "test" },
        health_check_ms: an_instance_of(Integer),
        logger: Pgbus.logger
      )
      expect(fake_listener).to have_received(:start)
      expect(consumer.instance_variable_get(:@notify_listener)).to be(fake_listener)
    end

    it "swallows listener startup errors and falls back to polling without crashing" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(consumer.config).to receive(:worker_notify_connection_options)
        .and_raise(StandardError, "no direct port configured")
      allow(Pgbus.logger).to receive(:error)

      expect { consumer.send(:start_notify_listener) }.not_to raise_error
      expect(consumer.instance_variable_get(:@notify_listener)).to be_nil
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "wake-on-notify integration" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }
    let(:wake_signal) { consumer.instance_variable_get(:@wake_signal) }

    it "WakeSignal#notify! interrupts the idle wait immediately" do
      # A real WakeSignal: waiting with a long timeout returns instantly once
      # notify! fires from another thread (what NotifyListener's on_wake does).
      waker = Thread.new do
        sleep 0.01
        wake_signal.notify!
      end
      expect(wake_signal.wait(timeout: 5)).to be true
      waker.join
    end

    it "consume waits on the wake_signal (not interruptible_sleep) when idle" do
      consumer.instance_variable_set(:@queue_names, ["q_orders"])
      allow(mock_client).to receive(:read_batch).and_return([])
      allow(wake_signal).to receive(:wait)

      consumer.send(:consume)

      expect(wake_signal).to have_received(:wait).with(timeout: a_kind_of(Numeric))
    end
  end

  describe "#shutdown stops the notify listener" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "stops the listener when one is attached" do
      consumer.instance_variable_set(:@notify_listener, fake_listener)
      consumer.send(:shutdown)
      expect(fake_listener).to have_received(:stop)
    end
  end
end
