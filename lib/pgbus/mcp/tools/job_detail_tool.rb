# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Inspect a single enqueued message by queue + msg_id. Returns its
      # metadata (read_ct, vt, enqueued_at, last_read_at); the body and headers
      # are redacted unless payloads are explicitly allowed. Maps to
      # DataSource#job_detail.
      class JobDetailTool < BaseTool
        tool_name "pgbus_job_detail"
        title "Pgbus Job Detail"
        description <<~DESC
          Inspect one enqueued message: read_ct, visibility timeout (vt),
          enqueued_at, last_read_at. Pass the full physical queue name and the
          numeric msg_id. The message body and headers are redacted unless the
          server allows payloads and include_payloads is set.
        DESC

        input_schema(
          properties: {
            queue: { type: "string", description: "Full physical queue name (e.g. \"pgbus_default\")." },
            msg_id: { type: "integer", description: "Numeric PGMQ message id." },
            include_payloads: {
              type: "boolean",
              description: "Include the raw message body/headers. Only honored when the server allows payloads."
            }
          },
          required: %w[queue msg_id]
        )

        def self.call(queue:, msg_id:, include_payloads: false, server_context: nil)
          data_source = data_source_from(server_context)
          detail = data_source.job_detail(queue, msg_id)
          return error_response("Message #{msg_id} not found in #{queue}") unless detail

          json_response({ job: detail }, server_context: server_context, include_payloads: include_payloads)
        end
      end
    end
  end
end
