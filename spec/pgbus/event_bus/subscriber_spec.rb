# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::EventBus::Subscriber do
  include PgmqDoubles

  let(:handler_class) do
    Class.new(Pgbus::EventBus::Handler) do
      def handle(event); end
    end
  end
  let(:mock_client) { build_mock_client }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe "#initialize" do
    context "with explicit queue_name" do
      subject(:subscriber) { described_class.new(pattern: "orders.#", handler_class: handler_class, queue_name: "my_queue") }

      it "uses the provided queue_name" do
        expect(subscriber.queue_name).to eq("my_queue")
      end

      it "stores the pattern" do
        expect(subscriber.pattern).to eq("orders.#")
      end

      it "stores the handler_class" do
        expect(subscriber.handler_class).to eq(handler_class)
      end
    end

    context "without explicit queue_name" do
      it "derives queue_name from handler class name" do
        stub_const("OrderCreatedHandler", handler_class)
        subscriber = described_class.new(pattern: "orders.created", handler_class: OrderCreatedHandler)

        expect(subscriber.queue_name).to eq("order_created_handler")
      end
    end
  end

  describe "queue name derivation" do
    it "converts CamelCase to snake_case" do
      stub_const("OrderCreatedHandler", handler_class)
      subscriber = described_class.new(pattern: "orders.created", handler_class: OrderCreatedHandler)

      expect(subscriber.queue_name).to eq("order_created_handler")
    end

    it "handles namespaced classes by replacing :: with underscore" do
      stub_const("MyApp::Events::OrderHandler", handler_class)
      subscriber = described_class.new(pattern: "orders.#", handler_class: MyApp::Events::OrderHandler)

      expect(subscriber.queue_name).to eq("my_app_events_order_handler")
    end

    it "handles consecutive capitals (acronyms)" do
      stub_const("APIEventHandler", handler_class)
      subscriber = described_class.new(pattern: "api.#", handler_class: APIEventHandler)

      expect(subscriber.queue_name).to eq("api_event_handler")
    end
  end

  describe "#setup!" do
    it "calls ensure_queue and bind_topic on the client" do
      stub_const("SetupTestHandler", handler_class)
      subscriber = described_class.new(pattern: "orders.#", handler_class: SetupTestHandler)

      subscriber.setup!

      expect(mock_client).to have_received(:ensure_queue).with("setup_test_handler")
      expect(mock_client).to have_received(:bind_topic).with("orders.#", "setup_test_handler")
    end

    it "uses explicit queue_name when provided" do
      subscriber = described_class.new(pattern: "payments.#", handler_class: handler_class, queue_name: "custom_payments")

      subscriber.setup!

      expect(mock_client).to have_received(:ensure_queue).with("custom_payments")
      expect(mock_client).to have_received(:bind_topic).with("payments.#", "custom_payments")
    end
  end
end
