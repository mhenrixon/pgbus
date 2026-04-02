# frozen_string_literal: true

require "json"

module Pgbus
  module Serializer
    module_function

    def serialize_job(active_job)
      Instrumentation.instrument("pgbus.serializer.serialize", kind: :job) do
        data = active_job.serialize
        # GlobalID is handled by ActiveJob's serialize — it converts AR objects
        # to GlobalID URIs automatically. We just JSON-encode the result.
        JSON.generate(data)
      end
    end

    def serialize_job_hash(active_job)
      Instrumentation.instrument("pgbus.serializer.serialize", kind: :job) do
        active_job.serialize
      end
    end

    def deserialize_job(json_string)
      Instrumentation.instrument("pgbus.serializer.deserialize", kind: :job) do
        data = JSON.parse(json_string)
        ActiveJob::Base.deserialize(data)
      end
    end

    def serialize_event(event)
      payload = event.respond_to?(:to_global_id) ? { "_global_id" => event.to_global_id.to_s } : event
      JSON.generate({
                      "event_id" => event.respond_to?(:event_id) ? event.event_id : SecureRandom.uuid,
                      "payload" => payload,
                      "published_at" => Time.now.utc.iso8601(6)
                    })
    end

    def deserialize_event(json_string)
      data = JSON.parse(json_string)
      payload = data["payload"]

      data["payload"] = locate_global_id(payload["_global_id"]) if payload.is_a?(Hash) && payload["_global_id"]

      Event.new(
        event_id: data["event_id"],
        payload: data["payload"],
        published_at: Time.parse(data["published_at"])
      )
    end

    # Locate a GlobalID with optional type restriction.
    # When allowed_global_id_models is configured, only those model classes
    # can be resolved — prevents loading arbitrary objects from crafted payloads.
    def locate_global_id(gid_string)
      gid = GlobalID.parse(gid_string)
      raise ArgumentError, "Invalid GlobalID: #{gid_string.inspect}" unless gid

      allowed = Pgbus.configuration.allowed_global_id_models
      if allowed&.empty?
        raise ArgumentError,
              "GlobalID deserialization is disabled (allowed_global_id_models is empty). " \
              "Set to nil to allow all models, or add permitted classes."
      end
      if allowed&.any? { |entry| !entry.is_a?(Class) && !entry.is_a?(Module) }
        raise ArgumentError,
              "allowed_global_id_models must contain Class/Module objects, " \
              "got: #{allowed.map(&:class).uniq.join(", ")}"
      end
      if allowed&.none? { |klass| gid.model_class <= klass }
        raise ArgumentError,
              "GlobalID model #{gid.model_class} is not in allowed_global_id_models. " \
              "Add it to Pgbus.configuration.allowed_global_id_models to permit deserialization."
      end

      GlobalID::Locator.locate(gid)
    end
  end
end
