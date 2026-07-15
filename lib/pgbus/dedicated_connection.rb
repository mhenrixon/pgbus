# frozen_string_literal: true

module Pgbus
  # Single choke point for opening a DEDICATED PG connection outside the
  # pgmq pools — the streamer's LISTEN connection and the worker
  # NotifyListener. Every such path MUST route through here (enforced by
  # spec/pgbus/pg_connect_guard_spec.rb): in :session GUC mode,
  # Configuration#forward_connection_variables leaves the database.yml
  # `variables:` hash on the connection options for the caller to apply
  # post-connect, and :variables is not a libpq keyword — a raw
  # PG.connect(**opts) fails with `invalid connection option "variables"`
  # (issue #352). This mirrors Client#wrap_session_gucs, which does the
  # same for pool connections: the GUCs are applied via post-connect SET
  # because a transaction-mode pooler rejects the libpq `options` startup
  # param (the reason :session mode exists).
  module DedicatedConnection
    module_function

    def connect(opts)
      require "pg" unless defined?(::PG::Connection)
      case opts
      when String then ::PG.connect(opts)
      when Hash then connect_from_hash(opts)
      else
        raise Pgbus::ConfigurationError,
              "Cannot build a dedicated PG connection from #{opts.class}. " \
              "Set database_url or connection_params so pgbus can open its own connection."
      end
    end

    def connect_from_hash(opts)
      variables = opts[:variables]
      conn = ::PG.connect(**opts.except(:variables))
      variables&.each { |name, value| conn.exec("SET #{name} = '#{value}'") }
      conn
    end
  end
end
