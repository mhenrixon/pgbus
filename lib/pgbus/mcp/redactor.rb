# frozen_string_literal: true

module Pgbus
  module MCP
    # Strips message bodies / headers / job arguments from DataSource result
    # hashes before they leave the process. Job payloads can carry PII or
    # secrets, so the diagnostic tools return metadata (ids, counts, ages,
    # read_ct, vt, queue names, job_class) by default and only include the
    # raw payload when an explicit, separately-gated flag is set.
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

      # Returns a copy of +hash+ with payload-bearing keys replaced by a
      # redaction marker. When +include_payloads+ is true the hash is returned
      # untouched. Non-hash arguments pass through unchanged.
      def redact(hash, include_payloads: false)
        return hash if include_payloads
        return hash unless hash.is_a?(Hash)

        hash.each_with_object({}) do |(key, value), result|
          result[key] = PAYLOAD_KEYS.include?(key.to_sym) && !value.nil? ? REDACTED : value
        end
      end

      # Redact every hash in an array of DataSource rows.
      def redact_all(rows, include_payloads: false)
        return rows if include_payloads

        Array(rows).map { |row| redact(row, include_payloads: include_payloads) }
      end
    end
  end
end
