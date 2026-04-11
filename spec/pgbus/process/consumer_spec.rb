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
end
