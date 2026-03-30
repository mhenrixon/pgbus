# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Serializer do
  describe ".serialize_job" do
    it "returns JSON string from active_job.serialize" do
      job = build_job_double(job_class: "MyWorker", queue_name: "critical")
      result = described_class.serialize_job(job)
      parsed = JSON.parse(result)

      expect(parsed["job_class"]).to eq("MyWorker")
      expect(parsed["queue_name"]).to eq("critical")
      expect(parsed["arguments"]).to eq([])
    end

    it "preserves the job_id from the job" do
      job_id = SecureRandom.uuid
      job = build_job_double(job_id: job_id)
      result = described_class.serialize_job(job)

      expect(JSON.parse(result)["job_id"]).to eq(job_id)
    end
  end

  describe ".deserialize_job" do
    it "delegates to ActiveJob::Base.deserialize" do
      job_data = { "job_class" => "TestJob", "job_id" => "abc-123", "queue_name" => "default", "arguments" => [] }
      json_string = JSON.generate(job_data)
      fake_job = double("ActiveJob::Base instance")

      # Inside Pgbus::Serializer, `ActiveJob::Base` resolves to `Pgbus::ActiveJob::Base`
      # because Zeitwerk defines Pgbus::ActiveJob as a namespace module.
      activejob_base = double("ActiveJob::Base class")
      allow(activejob_base).to receive(:deserialize).with(job_data).and_return(fake_job)
      stub_const("Pgbus::ActiveJob::Base", activejob_base)

      result = described_class.deserialize_job(json_string)
      expect(result).to eq(fake_job)
    end

    it "raises JSON::ParserError for invalid JSON" do
      expect { described_class.deserialize_job("not json") }.to raise_error(JSON::ParserError)
    end
  end

  describe ".serialize_event" do
    context "when event does not respond to to_global_id" do
      it "uses event_id from the event when available" do
        # A Hash does not respond_to?(:event_id) so a UUID is generated.
        # To test the event_id branch, define a simple class with event_id.
        event_obj = Class.new do
          def event_id = "evt-001"
          def to_json(*args) = { "type" => "order.created" }.to_json(*args)
        end.new

        result = JSON.parse(described_class.serialize_event(event_obj))

        expect(result["event_id"]).to eq("evt-001")
        expect(result["payload"]).to eq({ "type" => "order.created" })
        expect(result).to have_key("published_at")
      end

      it "generates a UUID event_id when event does not respond to event_id" do
        event = { "type" => "order.created", "data" => { "id" => 42 } }

        result = JSON.parse(described_class.serialize_event(event))

        expect(result["event_id"]).to match(/\A[0-9a-f-]{36}\z/)
        expect(result["payload"]).to eq(event)
      end

      it "includes published_at as ISO 8601 with microsecond precision" do
        frozen_time = Time.utc(2026, 3, 30, 12, 0, 0, 123_456)
        allow(Time).to receive(:now).and_return(frozen_time)

        event = { "type" => "test" }
        result = JSON.parse(described_class.serialize_event(event))

        expect(result["published_at"]).to eq("2026-03-30T12:00:00.123456Z")
      end
    end

    context "when event responds to to_global_id" do
      it "stores a _global_id payload" do
        global_id = double("GlobalID", to_s: "gid://app/Order/42")
        event_class = Struct.new(:event_id, :to_global_id)
        event = event_class.new(event_id: "evt-gid-1", to_global_id: global_id)

        result = JSON.parse(described_class.serialize_event(event))

        expect(result["event_id"]).to eq("evt-gid-1")
        expect(result["payload"]).to eq({ "_global_id" => "gid://app/Order/42" })
      end
    end
  end

  describe ".deserialize_event" do
    context "when payload does not contain _global_id" do
      it "returns an Event with the plain payload" do
        data = {
          "event_id" => "evt-plain",
          "payload" => { "type" => "order.created", "amount" => 99 },
          "published_at" => "2026-03-30T12:00:00.000000Z"
        }

        event = described_class.deserialize_event(JSON.generate(data))

        expect(event).to be_a(Pgbus::Event)
        expect(event.event_id).to eq("evt-plain")
        expect(event.payload).to eq({ "type" => "order.created", "amount" => 99 })
        expect(event.published_at).to be_a(Time)
        expect(event.published_at.year).to eq(2026)
      end
    end

    context "when payload contains _global_id" do
      it "resolves the object via GlobalID::Locator" do
        resolved_object = double("Order", id: 42)
        locator = double("GlobalID::Locator")
        allow(locator).to receive(:locate).with("gid://app/Order/42").and_return(resolved_object)
        stub_const("GlobalID::Locator", locator)

        data = {
          "event_id" => "evt-gid",
          "payload" => { "_global_id" => "gid://app/Order/42" },
          "published_at" => "2026-03-30T12:00:00.000000Z"
        }

        event = described_class.deserialize_event(JSON.generate(data))

        expect(event.payload).to eq(resolved_object)
        expect(event.event_id).to eq("evt-gid")
      end
    end

    context "when payload is a string" do
      it "keeps the string payload as-is" do
        data = {
          "event_id" => "evt-str",
          "payload" => "just a string",
          "published_at" => "2026-03-30T10:00:00.000000Z"
        }

        event = described_class.deserialize_event(JSON.generate(data))

        expect(event.payload).to eq("just a string")
      end
    end

    it "raises JSON::ParserError for invalid JSON" do
      expect { described_class.deserialize_event("{bad") }.to raise_error(JSON::ParserError)
    end
  end
end
