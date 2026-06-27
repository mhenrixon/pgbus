# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Lists every pgbus process (worker/consumer/dispatcher/scheduler/
      # supervisor/outbox/streamer) with kind, pid, heartbeat age, derived
      # status and the claim-loop "stalled" verdict. Maps to
      # DataSource#processes — which already computes :status as :healthy,
      # :stale, or :stalled (the silent-wedge signal from #179/#181).
      class ProcessesTool < BaseTool
        tool_name "pgbus_processes"
        title "Pgbus Processes"
        description <<~DESC
          List every pgbus process with its kind (worker, consumer,
          dispatcher, scheduler, supervisor, outbox, streamer), hostname, pid,
          last heartbeat time, and derived status. Status is "healthy",
          "stale" (no recent heartbeat), or "stalled" (worker heart-beating but
          its claim loop has stopped advancing — the silent-wedge condition).
          Use this to answer "are workers alive but not claiming?".
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response(processes: data_source.processes)
        end
      end
    end
  end
end
