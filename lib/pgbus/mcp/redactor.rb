# frozen_string_literal: true

module Pgbus
  module MCP
    # Strips message bodies / headers / job arguments from tool responses
    # before they leave the process. Job payloads can carry PII or secrets, so
    # the diagnostic tools return metadata (ids, counts, ages, read_ct, vt,
    # queue names, job_class) by default and only include the raw payload when
    # an explicit, separately-gated flag is set.
    #
    # +deep_redact+ is applied centrally at the response boundary
    # (BaseTool.json_response), so redaction is fail-safe: a tool author cannot
    # leak a payload by forgetting to redact — they have to opt in.
    #
    # See issue #180: "No sensitive payload leakage — diagnostic tools should
    # return metadata ... and redact or omit message payloads unless an
    # explicit, separately-gated flag is set."
    module Redactor
      module_function

      # Keys whose values are message bodies / headers and must never be
      # returned unless payloads are explicitly allowed.
      PAYLOAD_KEYS = %i[message headers payload arguments].freeze

      # Replacement marker so the agent can see a field exists but was hidden,
      # rather than the field silently vanishing.
      REDACTED = "[redacted]"

      # True if +key+ names a payload-bearing field. Tolerates string or symbol
      # keys and never raises on a non-symbolizable key.
      def payload_key?(key)
        PAYLOAD_KEYS.include?(key.respond_to?(:to_sym) ? key.to_sym : key)
      end

      # Recursively redact payload-bearing keys anywhere in a nested structure
      # of hashes and arrays. This is the fail-safe applied at the response
      # boundary (BaseTool.json_response): a tool that returns a payload key —
      # at any depth, even one its author forgot about — has it stripped unless
      # payloads are explicitly allowed for the call.
      #
      # A payload key is redacted only when its value is a leaf (the serialized
      # message body / headers string). When the value is a Hash or Array the
      # key is an envelope (e.g. a {message: {...}} detail wrapper, not a raw
      # body), so we descend into it instead of blanking the whole subtree.
      def deep_redact(value, include_payloads: false)
        return value if include_payloads

        case value
        when Hash
          value.each_with_object({}) do |(key, nested), result|
            result[key] =
              if payload_key?(key) && leaf?(nested) && !nested.nil?
                REDACTED
              else
                deep_redact(nested, include_payloads: include_payloads)
              end
          end
        when Array
          value.map { |element| deep_redact(element, include_payloads: include_payloads) }
        else
          value
        end
      end

      # A leaf is anything that isn't a structure we recurse into — i.e. the
      # actual serialized payload value rather than an envelope hash/array.
      def leaf?(value)
        !value.is_a?(Hash) && !value.is_a?(Array)
      end
    end
  end
end
