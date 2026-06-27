# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Per-queue detail: metrics, paused state, and table health (dead tuples,
      # bloat, vacuum age). Maps to DataSource#queue_detail + #queue_paused? +
      # #queue_health_detail. The +name+ is the full physical queue name as
      # returned by pgbus_queues (e.g. "pgbus_default"), not the logical name.
      class QueueDetailTool < BaseTool
        tool_name "pgbus_queue_detail"
        title "Pgbus Queue Detail"
        description <<~DESC
          Detailed status for a single queue: depth, visible count, message
          ages, lifetime total, whether the queue is paused, and per-table
          health (live/dead tuples, bloat ratio, last vacuum age). Pass the
          full physical queue name as shown by pgbus_queues (e.g.
          "pgbus_default").
        DESC

        input_schema(
          properties: {
            name: {
              type: "string",
              description: "Full physical queue name, e.g. \"pgbus_default\"."
            }
          },
          required: ["name"]
        )

        def self.call(name:, server_context: nil)
          data_source = data_source_from(server_context)
          detail = data_source.queue_detail(name)
          return error_response("Queue not found: #{name}") unless detail

          json_response(
            detail.merge(
              paused: data_source.queue_paused?(name),
              health: data_source.queue_health_detail(name)
            )
          )
        end
      end
    end
  end
end
