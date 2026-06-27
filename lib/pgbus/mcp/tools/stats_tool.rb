# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Recent job status counts + summary (success / failed / dead-lettered,
      # durations). Maps to DataSource#job_status_counts + #job_stats_summary.
      class StatsTool < BaseTool
        tool_name "pgbus_stats"
        title "Pgbus Stats"
        description <<~DESC
          Recent job statistics over the last N minutes (default 60, max 1440):
          status counts (success/failed/dead_lettered) and a summary including
          average and max duration. Use this to gauge error rates and latency.
        DESC

        MAX_MINUTES = 1440

        input_schema(
          properties: {
            minutes: { type: "integer", description: "Look-back window in minutes (default 60, max 1440).", minimum: 1 }
          },
          required: []
        )

        def self.call(minutes: 60, server_context: nil)
          data_source = data_source_from(server_context)
          window = minutes.to_i.clamp(1, MAX_MINUTES)
          json_response(
            {
              minutes: window,
              status_counts: data_source.job_status_counts(minutes: window),
              summary: data_source.job_stats_summary(minutes: window)
            }
          )
        end
      end
    end
  end
end
