# frozen_string_literal: true

module Pgbus
  class Event
    attr_reader :event_id, :payload, :published_at, :routing_key, :headers

    def initialize(event_id:, payload:, published_at: nil, routing_key: nil, headers: nil)
      @event_id = event_id
      @payload = payload
      @published_at = published_at || Time.now.utc
      @routing_key = routing_key
      @headers = headers || {}
    end

    def [](key)
      payload.is_a?(Hash) ? payload[key.to_s] : nil
    end

    def to_h
      {
        "event_id" => event_id,
        "payload" => payload,
        "published_at" => published_at.iso8601(6),
        "routing_key" => routing_key,
        "headers" => headers
      }
    end
  end
end
