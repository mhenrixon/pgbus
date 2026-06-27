# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Inspect a single dead-letter message by msg_id (searched across all
      # *_dlq queues). Maps to DataSource#dlq_message_detail. Payload redacted
      # by default.
      class DlqDetailTool < BaseTool
        tool_name "pgbus_dlq_detail"
        title "Pgbus Dead-Letter Detail"
        description <<~DESC
          Inspect one dead-letter message by its numeric msg_id (searched
          across all "_dlq" queues): read_ct, vt, enqueued_at, source queue.
          The message body and headers are redacted unless the server allows
          payloads and include_payloads is set.
        DESC

        input_schema(
          properties: {
            msg_id: { type: "integer", description: "Numeric PGMQ message id." },
            include_payloads: {
              type: "boolean",
              description: "Include the raw message body/headers. Only honored when the server allows payloads."
            }
          },
          required: %w[msg_id]
        )

        def self.call(msg_id:, include_payloads: false, server_context: nil)
          data_source = data_source_from(server_context)
          detail = data_source.dlq_message_detail(msg_id)
          return error_response("Dead-letter message #{msg_id} not found") unless detail

          allow = payloads_allowed?(server_context, include_payloads)
          json_response(message: Redactor.redact(detail, include_payloads: allow))
        end
      end
    end
  end
end
