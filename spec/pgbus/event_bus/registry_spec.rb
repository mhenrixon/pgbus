# frozen_string_literal: true

require "spec_helper"

require "singleton"

RSpec.describe Pgbus::EventBus::Registry do
  subject(:registry) { described_class.instance }

  # Use a test double for handler class
  let(:handler_class) do
    klass = Class.new(Pgbus::EventBus::Handler)
    stub_const("TestHandler", klass)
    klass
  end

  before { registry.clear! }

  describe "#subscribe" do
    it "registers a subscriber" do
      registry.subscribe("orders.#", handler_class)
      expect(registry.subscribers.size).to eq(1)
    end

    it "returns a Subscriber" do
      subscriber = registry.subscribe("orders.created", handler_class)
      expect(subscriber).to be_a(Pgbus::EventBus::Subscriber)
      expect(subscriber.pattern).to eq("orders.created")
    end
  end

  describe "#handlers_for" do
    before do
      registry.subscribe("orders.#", handler_class)
    end

    it "matches exact routing keys" do
      handlers = registry.handlers_for("orders.created")
      expect(handlers.size).to eq(1)
    end

    it "matches wildcard patterns" do
      handlers = registry.handlers_for("orders.updated.shipping")
      expect(handlers.size).to eq(1)
    end

    it "does not match unrelated routing keys" do
      handlers = registry.handlers_for("users.created")
      expect(handlers).to be_empty
    end
  end

  describe "#event_queue_names" do
    it "returns the prefixed physical queue name for each subscriber (issue #333)" do
      registry.subscribe("orders.#", handler_class, queue_name: "orders_handler")
      prefix = Pgbus.configuration.queue_prefix

      expect(registry.event_queue_names).to contain_exactly("#{prefix}_orders_handler")
    end

    it "returns an empty set when no subscribers are registered" do
      expect(registry.event_queue_names).to be_empty
    end
  end

  describe "#clear!" do
    it "removes all subscribers" do
      registry.subscribe("orders.#", handler_class)
      registry.clear!
      expect(registry.subscribers).to be_empty
    end
  end

  describe "#setup_all! (issue #334)" do
    let(:subscriber) do
      registry.subscribe("orders.#", handler_class, queue_name: "orders_handler")
    end

    it "sets up every subscriber by default" do
      allow(subscriber).to receive(:setup!)
      registry.setup_all!
      expect(subscriber).to have_received(:setup!)
    end

    context "with safe: true" do
      it "swallows a connection error instead of crashing boot" do
        allow(subscriber).to receive(:setup!).and_raise(real_pgmq_connection_error, "db down")
        allow(Pgbus.logger).to receive(:warn)

        expect { registry.setup_all!(safe: true) }.not_to raise_error
        expect(Pgbus.logger).to have_received(:warn)
      end

      it "skips entirely during a schema/db: rake context (no connection opened)" do
        allow(registry).to receive(:schema_task_context?).and_return(true)
        allow(subscriber).to receive(:setup!)

        registry.setup_all!(safe: true)

        expect(subscriber).not_to have_received(:setup!)
      end

      it "still sets up normally outside a schema context" do
        allow(registry).to receive(:schema_task_context?).and_return(false)
        allow(subscriber).to receive(:setup!)

        registry.setup_all!(safe: true)

        expect(subscriber).to have_received(:setup!)
      end
    end
  end
end
