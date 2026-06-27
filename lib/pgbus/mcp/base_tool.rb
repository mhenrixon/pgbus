# frozen_string_literal: true

require "json"

module Pgbus
  module MCP
    # Base class for every pgbus diagnostic tool. Provides the shared
    # read-only annotation, a JSON response helper, and access to the
    # DataSource the tool delegates to.
    #
    # The DataSource is pulled from +server_context[:data_source]+ so the
    # server can inject a configured instance (and tests can inject a double).
    # Every subclass is read-only by contract — see issue #180 security
    # requirements. Write/admin tools, if ever added, must live in a separate,
    # explicitly opt-in surface and never inherit from this class.
    class BaseTool < ::MCP::Tool
      # Marks the tool read-only and non-destructive so MCP clients can show
      # the right affordance and never treat a call as a mutation.
      annotations(
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true,
        open_world_hint: false
      )

      class << self
        # MCP::Tool.inherited resets @annotations_value to nil on every
        # subclass, so the read_only_hint set on BaseTool would not reach the
        # concrete tools. Fall back to the nearest ancestor that defined
        # annotations so all tools inherit the read-only contract without
        # repeating it. (annotations_value is what MCP::Tool#to_h reads.)
        def annotations_value
          super || (superclass.respond_to?(:annotations_value) ? superclass.annotations_value : nil)
        end

        # Pull the injected DataSource (or build a default one). Kept as a
        # class method because MCP tool entry points (`self.call`) are class
        # methods.
        def data_source_from(server_context)
          (server_context && server_context[:data_source]) || Pgbus::Web::DataSource.new
        end

        # Whether payloads may be returned for this call. Honors a per-call
        # `include_payloads` argument only when the server was started with
        # payloads globally allowed (`server_context[:allow_payloads]`).
        # Defaults to false on both axes so nothing leaks by accident.
        def payloads_allowed?(server_context, include_payloads)
          return false unless server_context && server_context[:allow_payloads]

          !!include_payloads
        end

        # Wrap any Ruby value as a single text-content JSON response.
        def json_response(value)
          ::MCP::Tool::Response.new([{ type: "text", text: JSON.pretty_generate(value) }])
        end

        # Wrap an error message as an MCP error response (isError: true) so the
        # client surfaces it as a tool failure rather than a normal result.
        def error_response(message)
          ::MCP::Tool::Response.new(
            [{ type: "text", text: message }],
            error: true
          )
        end
      end
    end
  end
end
