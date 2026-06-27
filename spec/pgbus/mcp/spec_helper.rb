# frozen_string_literal: true

# The MCP subsystem is loaded explicitly (it's excluded from Zeitwerk because
# its tools subclass the optional `mcp` gem's MCP::Tool). Load it once for the
# whole MCP spec suite.
require "spec_helper"
Pgbus::MCP.load!
