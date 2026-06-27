# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Lists active job uniqueness locks (the leaked-uniqueness-lock
      # diagnostic). Maps to DataSource#job_locks — lock_key, queue_name,
      # msg_id, created_at, age_seconds. No payloads involved.
      class LocksTool < BaseTool
        tool_name "pgbus_locks"
        title "Pgbus Locks"
        description <<~DESC
          List active job uniqueness locks with their lock_key, queue_name,
          msg_id, and age in seconds. A lock that outlives its message
          indicates a leaked uniqueness key blocking re-enqueues. Returns the
          100 most recent locks.
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response({ locks: data_source.job_locks })
        end
      end
    end
  end
end
