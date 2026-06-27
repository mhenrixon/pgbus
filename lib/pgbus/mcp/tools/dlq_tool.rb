# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Paginated dead-letter queue inspection across all *_dlq queues.
      # Maps to DataSource#dlq_messages. Payloads redacted by default.
      class DlqTool < BaseTool
        tool_name "pgbus_dlq"
        title "Pgbus Dead-Letter Queue"
        description <<~DESC
          List messages sitting in dead-letter queues (queues whose name ends
          in "_dlq") with read_ct, vt and enqueued_at. Paginated (default 25,
          max 100). Message bodies and headers are redacted unless the server
          allows payloads and include_payloads is set.
        DESC

        MAX_PER_PAGE = 100

        input_schema(
          properties: {
            page: { type: "integer", description: "1-based page number (default 1).", minimum: 1 },
            per_page: { type: "integer", description: "Rows per page (default 25, max 100).", minimum: 1 },
            include_payloads: {
              type: "boolean",
              description: "Include raw message bodies/headers. Only honored when the server allows payloads."
            }
          },
          required: []
        )

        def self.call(page: 1, per_page: 25, include_payloads: false, server_context: nil)
          data_source = data_source_from(server_context)
          per_page = per_page.to_i.clamp(1, MAX_PER_PAGE)
          page = [page.to_i, 1].max
          rows = data_source.dlq_messages(page: page, per_page: per_page)

          json_response(
            { page: page, per_page: per_page, messages: rows },
            server_context: server_context,
            include_payloads: include_payloads
          )
        end
      end
    end
  end
end
