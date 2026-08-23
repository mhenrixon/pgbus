# frozen_string_literal: true

require "spec_helper"
require "active_job"

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
    it "delegates to ::ActiveJob::Base.deserialize after the allowlist gate" do
      job_data = { "job_class" => "TestJob", "job_id" => "abc-123", "queue_name" => "default", "arguments" => [] }
      json_string = JSON.generate(job_data)
      fake_job = double("ActiveJob::Base instance")

      # Use the top-level constant — bare ActiveJob::Base can resolve to
      # Pgbus::ActiveJob::Base under Zeitwerk (issue #368 path fix).
      allow(ActiveJob::Base).to receive(:deserialize).with(job_data).and_return(fake_job)

      result = described_class.deserialize_job(json_string)
      expect(result).to eq(fake_job)
    end

    it "raises JSON::ParserError for invalid JSON" do
      expect { described_class.deserialize_job("not json") }.to raise_error(JSON::ParserError)
    end
  end

  describe ".deserialize_job_data (issue #368)" do
    let(:job_data) do
      { "job_class" => "TestJob", "job_id" => "abc-123", "queue_name" => "default", "arguments" => [] }
    end
    let(:fake_job) { double("ActiveJob::Base instance") }

    before do
      allow(ActiveJob::Base).to receive(:deserialize).with(job_data).and_return(fake_job)
    end

    it "deserializes a plain job hash via ::ActiveJob::Base" do
      expect(described_class.deserialize_job_data(job_data)).to eq(fake_job)
    end

    it "rejects a disallowed GlobalID inside persisted Current attributes (issue #430)" do
      stub_const("Order", Class.new)
      stub_const("Secret", Class.new)
      Pgbus.configuration.allowed_global_id_models = [Order]
      tagged = job_data.merge("pgbus_current" => { "Current" => { "tenant" => { "_aj_globalid" => "gid://pgbus-test/Secret/1" } } })

      expect { described_class.deserialize_job_data(tagged) }
        .to raise_error(Pgbus::SerializationError, /not in allowed_global_id_models/)
    ensure
      Pgbus.configuration.allowed_global_id_models = nil
    end

    it "skips the allowlist walk when allowed_global_id_models is nil" do
      Pgbus.configuration.allowed_global_id_models = nil
      allow(described_class).to receive(:assert_allowed_global_id!).and_call_original

      described_class.deserialize_job_data(job_data)

      expect(described_class).not_to have_received(:assert_allowed_global_id!)
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
        gid_uri = "gid://pgbus-test/Order/42"
        resolved_object = double("Order", id: 42)

        # Stub at the locator level — GlobalID.parse works with the real gem
        allow(GlobalID::Locator).to receive(:locate).and_return(resolved_object)

        data = {
          "event_id" => "evt-gid",
          "payload" => { "_global_id" => gid_uri },
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
