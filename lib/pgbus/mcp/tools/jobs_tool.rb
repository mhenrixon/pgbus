# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Paginated list of enqueued jobs/messages for a queue (or across all
      # non-DLQ queues). Returns metadata — msg_id, read_ct, vt, enqueued_at,
      # queue_name — with message bodies and headers redacted unless payloads
      # are explicitly allowed. Maps to DataSource#jobs.
      class JobsTool < BaseTool
        tool_name "pgbus_jobs"
        title "Pgbus Jobs"
        description <<~DESC
          List enqueued jobs/messages with read_ct, visibility timeout (vt) and
          enqueued_at. Pass a full physical queue name to scope to one queue,
          or omit it to span all non-DLQ queues. Paginated (default 25 per
          page, capped at 100). Message bodies and headers are redacted unless
          the server was started with payloads allowed and include_payloads is
          set.
        DESC

        MAX_PER_PAGE = 100

        input_schema(
          properties: {
            queue: {
              type: "string",
              description: "Full physical queue name (e.g. \"pgbus_default\"). Omit to span all non-DLQ queues."
            },
            page: { type: "integer", description: "1-based page number (default 1).", minimum: 1 },
            per_page: { type: "integer", description: "Rows per page (default 25, max 100).", minimum: 1 },
            include_payloads: {
              type: "boolean",
              description: "Include raw message bodies/headers. Only honored when the server allows payloads."
            }
          },
          required: []
        )

        def self.call(queue: nil, page: 1, per_page: 25, include_payloads: false, server_context: nil)
          data_source = data_source_from(server_context)
          per_page = per_page.to_i.clamp(1, MAX_PER_PAGE)
          page = [page.to_i, 1].max
          rows = data_source.jobs(queue_name: queue, page: page, per_page: per_page)

          allow = payloads_allowed?(server_context, include_payloads)
          json_response(
            queue: queue,
            page: page,
            per_page: per_page,
            jobs: Redactor.redact_all(rows, include_payloads: allow)
          )
        end
      end
    end
  end
end
