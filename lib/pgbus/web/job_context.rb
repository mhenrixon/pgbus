# frozen_string_literal: true

require "json"

module Pgbus
  module Web
    # Presents the Current attributes persisted in a job payload
    # (Pgbus::CurrentAttributes::METADATA_KEY) for the dashboard's Context
    # card: { "Current" => { "tenant" => "gid://app/Tenant/42", ... } }.
    #
    # Pure presentation: ActiveJob argument encodings are unwrapped to display
    # strings (a GlobalID shows its gid, a serialized Symbol/Time its value) —
    # nothing is constantized or located. Callers pass the payload through
    # PayloadFilter first (ApplicationHelper#pgbus_parse_message does), so
    # sensitive values arrive already redacted.
    module JobContext
      AJ_GLOBALID = "_aj_globalid"
      AJ_SERIALIZED = "_aj_serialized"
      AJ_META_PREFIX = "_aj_"

      module_function

      def from_payload(payload)
        data = parse(payload)
        stored = data && data[Pgbus::CurrentAttributes::METADATA_KEY]
        return nil unless stored.is_a?(Hash) && stored.any?

        stored.to_h do |klass_name, attrs|
          [klass_name.to_s, present_attrs(attrs)]
        end
      end

      def present_attrs(attrs)
        return {} unless attrs.is_a?(Hash)

        attrs.each_with_object({}) do |(name, value), out|
          next if name.to_s.start_with?(AJ_META_PREFIX)

          out[name.to_s] = display(value)
        end
      end

      # String form of a value for the card. Hashes/arrays that are ActiveJob
      # encodings collapse to their payload; anything else inspects.
      def display(value)
        unwrapped = unwrap(value)
        unwrapped.is_a?(String) ? unwrapped : unwrapped.inspect
      end

      def unwrap(value)
        case value
        when Hash
          return value[AJ_GLOBALID] if value.key?(AJ_GLOBALID)
          return unwrap(value["value"]) if value.key?(AJ_SERIALIZED) && value.key?("value")

          value.each_with_object({}) do |(k, v), out|
            next if k.to_s.start_with?(AJ_META_PREFIX)

            out[k] = unwrap(v)
          end
        when Array
          value.map { |v| unwrap(v) }
        else
          value
        end
      end

      def parse(payload)
        case payload
        when Hash then payload
        when String then JSON.parse(payload)
        end
      rescue JSON::ParserError
        nil
      end
      private_class_method :parse
    end
  end
end
