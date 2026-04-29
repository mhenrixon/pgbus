# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Pgbus::EventBus::Handler do
  include PgmqDoubles

  let(:event_id) { SecureRandom.uuid }
  let(:published_at) { Time.now.utc.iso8601(6) }
  let(:raw_payload) { { "key" => "value" } }
  let(:raw_message) do
    {
      "event_id" => event_id,
      "payload" => raw_payload,
      "published_at" => published_at
    }.to_json
  end
  let(:message) { build_message_double(msg_id: 1, message: raw_message) }

  describe "concrete subclass that implements #handle" do
    let(:handler_class) do
      Class.new(described_class) do
        attr_reader :received_event

        def handle(event)
          @received_event = event
        end
      end
    end
    let(:handler) { handler_class.new }

    describe "#process" do
      it "parses JSON message, builds event, calls handle, and returns :handled" do
        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.received_event).to be_a(Pgbus::Event)
        expect(handler.received_event.event_id).to eq(event_id)
        expect(handler.received_event.payload).to eq(raw_payload)
      end

      it "parses published_at into a Time object" do
        handler.process(message)

        expect(handler.received_event.published_at).to be_a(Time)
      end

      it "handles nil published_at gracefully" do
        raw = { "event_id" => event_id, "payload" => raw_payload, "published_at" => nil }.to_json
        msg = build_message_double(msg_id: 2, message: raw)

        handler.process(msg)

        expect(handler.received_event.published_at).to be_a(Time)
      end
    end

    describe "GlobalID payload resolution" do
      let(:resolved_object) { double("User", id: 42) }
      let(:gid_uri) { "gid://pgbus-test/User/42" }
      let(:global_id_payload) { { "_global_id" => gid_uri } }
      let(:raw_message) do
        {
          "event_id" => event_id,
          "payload" => global_id_payload,
          "published_at" => published_at
        }.to_json
      end

      before do
        allow(GlobalID::Locator).to receive(:locate).and_return(resolved_object)
      end

      it "resolves GlobalID payloads via GlobalID::Locator" do
        handler.process(message)

        expect(handler.received_event.payload).to eq(resolved_object)
        expect(GlobalID::Locator).to have_received(:locate)
      end
    end

    describe "instrumentation via ActiveSupport::Notifications" do
      before do
        stub_const("ActiveSupport::Notifications", double("AS::Notifications"))
        allow(ActiveSupport::Notifications).to receive(:instrument)
      end

      it "instruments pgbus.event_processed after handling" do
        stub_const("TestHandler", handler_class)
        test_handler = TestHandler.new

        test_handler.process(message)

        expect(ActiveSupport::Notifications).to have_received(:instrument).with(
          "pgbus.event_processed",
          event_id: event_id,
          handler: "TestHandler"
        )
      end
    end
  end

  describe ".idempotent! / .idempotent?" do
    it "defaults to not idempotent" do
      klass = Class.new(described_class)
      expect(klass.idempotent?).to be false
    end

    it "becomes idempotent after calling .idempotent!" do
      klass = Class.new(described_class) do
        idempotent!
      end
      expect(klass.idempotent?).to be true
    end

    it "does not affect sibling subclasses" do
      idempotent_klass = Class.new(described_class) { idempotent! }
      normal_klass = Class.new(described_class)

      expect(idempotent_klass.idempotent?).to be true
      expect(normal_klass.idempotent?).to be false
    end
  end

  describe "idempotent handler" do
    let(:handler_class) do
      Class.new(described_class) do
        idempotent!

        def handle(event)
          # no-op
        end
      end
    end
    let(:handler) { handler_class.new }
    let(:insert_result) { double("InsertAll::Result", rows: [[1]]) }
    let(:empty_result) { double("InsertAll::Result", rows: []) }

    it "returns :handled when event was atomically claimed" do
      allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(insert_result)

      result = handler.process(message)

      expect(result).to eq(:handled)
      expect(Pgbus::ProcessedEvent).to have_received(:insert).with(
        hash_including(event_id: event_id, handler_class: handler_class.name),
        unique_by: %i[event_id handler_class]
      )
    end

    it "returns :skipped when event was already claimed by another handler" do
      allow(Pgbus::ProcessedEvent).to receive(:insert).and_return(empty_result)

      result = handler.process(message)

      expect(result).to eq(:skipped)
    end
  end

  describe "#handle (base class)" do
    it "raises NotImplementedError when not overridden" do
      handler = described_class.new

      event = Pgbus::Event.new(event_id: event_id, payload: raw_payload)
      expect { handler.handle(event) }.to raise_error(NotImplementedError, /must implement #handle/)
    end
  end

  describe "Rails executor wrapping" do
    let(:handler_class) do
      Class.new(described_class) do
        attr_reader :received_event

        def handle(event)
          @received_event = event
        end
      end
    end
    let(:handler) { handler_class.new }

    context "when Rails is loaded" do
      let(:mock_executor_wrapper) { double("executor") }
      let(:mock_reloader_wrapper) { double("reloader") }
      let(:mock_app_config) { double("config") }
      let(:mock_app) do
        double("Rails.application",
               executor: mock_executor_wrapper,
               reloader: mock_reloader_wrapper,
               config: mock_app_config)
      end

      def stub_rails!
        rails = double("Rails", application: mock_app)
        allow(rails).to receive(:respond_to?) { |name, *| name == :application }
        stub_const("Rails", rails)
      end

      before do
        allow(mock_executor_wrapper).to receive(:wrap).and_yield
        allow(mock_reloader_wrapper).to receive(:wrap).and_yield
      end

      context "when enable_reloading is true (development)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(true)
          stub_rails!
        end

        it "wraps process in reloader.wrap so handlers see code changes" do
          handler.process(message)

          expect(mock_reloader_wrapper).to have_received(:wrap)
          expect(mock_executor_wrapper).not_to have_received(:wrap)
          expect(handler.received_event.event_id).to eq(event_id)
        end
      end

      context "when enable_reloading is false (production)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(true)
          allow(mock_app_config).to receive(:enable_reloading).and_return(false)
          stub_rails!
        end

        it "wraps process in executor.wrap" do
          handler.process(message)

          expect(mock_executor_wrapper).to have_received(:wrap)
          expect(mock_reloader_wrapper).not_to have_received(:wrap)
        end
      end

      context "when config does not respond to enable_reloading (Rails < 7.1)" do
        before do
          allow(mock_app_config).to receive(:respond_to?).with(:enable_reloading).and_return(false)
          stub_rails!
        end

        it "falls back to !cache_classes — reloader.wrap when cache_classes is false" do
          allow(mock_app_config).to receive(:cache_classes).and_return(false)

          handler.process(message)

          expect(mock_reloader_wrapper).to have_received(:wrap)
        end

        it "falls back to !cache_classes — executor.wrap when cache_classes is true" do
          allow(mock_app_config).to receive(:cache_classes).and_return(true)

          handler.process(message)

          expect(mock_executor_wrapper).to have_received(:wrap)
        end
      end
    end

    context "when Rails is not loaded" do
      it "runs the handler without a wrapper (no-op)" do
        hide_const("Rails") if defined?(Rails)

        result = handler.process(message)

        expect(result).to eq(:handled)
        expect(handler.received_event.event_id).to eq(event_id)
      end
    end
  end
end
