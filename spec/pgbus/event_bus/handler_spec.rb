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
      let(:global_id_payload) { { "_global_id" => "gid://app/User/42" } }
      let(:raw_message) do
        {
          "event_id" => event_id,
          "payload" => global_id_payload,
          "published_at" => published_at
        }.to_json
      end

      before do
        stub_const("GlobalID::Locator", double("GlobalID::Locator"))
        allow(GlobalID::Locator).to receive(:locate).with("gid://app/User/42").and_return(resolved_object)
      end

      it "resolves GlobalID payloads via GlobalID::Locator" do
        handler.process(message)

        expect(handler.received_event.payload).to eq(resolved_object)
        expect(GlobalID::Locator).to have_received(:locate).with("gid://app/User/42")
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
    let(:connection) { double("AR::Connection") }
    let(:handler_class) do
      Class.new(described_class) do
        idempotent!

        def handle(event)
          # no-op
        end
      end
    end
    let(:handler) { handler_class.new }

    before do
      stub_const("ActiveRecord::Base", double("AR::Base", connection: connection))
      allow(connection).to receive_messages(select_value: nil, exec_insert: nil)
    end

    it "returns :handled when event has not been processed" do
      result = handler.process(message)

      expect(result).to eq(:handled)
      expect(connection).to have_received(:select_value)
      expect(connection).to have_received(:exec_insert)
    end

    it "returns :skipped when event was already processed" do
      allow(connection).to receive(:select_value).and_return(1)

      result = handler.process(message)

      expect(result).to eq(:skipped)
      expect(connection).to have_received(:select_value)
      expect(connection).not_to have_received(:exec_insert)
    end
  end

  describe "#handle (base class)" do
    it "raises NotImplementedError when not overridden" do
      handler = described_class.new

      event = Pgbus::Event.new(event_id: event_id, payload: raw_payload)
      expect { handler.handle(event) }.to raise_error(NotImplementedError, /must implement #handle/)
    end
  end
end
