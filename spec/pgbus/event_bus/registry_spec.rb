# frozen_string_literal: true

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

  describe "#clear!" do
    it "removes all subscribers" do
      registry.subscribe("orders.#", handler_class)
      registry.clear!
      expect(registry.subscribers).to be_empty
    end
  end
end
