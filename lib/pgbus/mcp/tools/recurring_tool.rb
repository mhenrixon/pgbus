# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Recurring task schedule with last-run / next-run times. Maps to
      # DataSource#recurring_tasks. The :arguments field is not part of the
      # list payload, so no redaction is needed here.
      class RecurringTool < BaseTool
        tool_name "pgbus_recurring"
        title "Pgbus Recurring Tasks"
        description <<~DESC
          List recurring (cron) tasks with their schedule, human-readable
          schedule, queue, enabled state, last_run_at and next_run_at. Use this
          to confirm scheduled work is firing on time.
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response({ recurring_tasks: data_source.recurring_tasks })
        end
      end
    end
  end
end
