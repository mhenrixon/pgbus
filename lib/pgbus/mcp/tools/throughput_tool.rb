# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Recent job throughput as a time series. Maps to
      # DataSource#job_throughput. Used to answer "is the system draining?".
      class ThroughputTool < BaseTool
        tool_name "pgbus_throughput"
        title "Pgbus Throughput"
        description <<~DESC
          Recent job throughput as a time series of {time, count} buckets over
          the last N minutes (default 60, max 1440). Use this to confirm the
          system is draining — a flat-zero series alongside a backlog is the
          wedge signature.
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
          json_response(minutes: window, throughput: data_source.job_throughput(minutes: window))
        end
      end
    end
  end
end
