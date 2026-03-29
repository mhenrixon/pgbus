# frozen_string_literal: true

require "json"

module Pgbus
  module Serializer
    module_function

    def serialize_job(active_job)
      data = active_job.serialize
      # GlobalID is handled by ActiveJob's serialize — it converts AR objects
      # to GlobalID URIs automatically. We just JSON-encode the result.
      JSON.generate(data)
    end

    def deserialize_job(json_string)
      data = JSON.parse(json_string)
      ActiveJob::Base.deserialize(data)
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

      data["payload"] = GlobalID::Locator.locate(payload["_global_id"]) if payload.is_a?(Hash) && payload["_global_id"]

      Event.new(
        event_id: data["event_id"],
        payload: data["payload"],
        published_at: Time.parse(data["published_at"])
      )
    end
  end
end
