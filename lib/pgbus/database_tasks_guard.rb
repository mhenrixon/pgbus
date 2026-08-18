# frozen_string_literal: true

module Pgbus
  # Prepended onto ActiveRecord::Tasks::DatabaseTasks' singleton class by the
  # engine (initializer "pgbus.db") so every database purge/drop first
  # disconnects the gem's own BusRecord pools.
  #
  # Why: with pgbus on a dedicated database (config.connects_to), any
  # boot-time touch of a Pgbus model leaves an idle session on that database
  # for the life of the process. Rails' purge/drop only disconnects the
  # connection it establishes for the target db_config — it knows nothing
  # about gem-owned pools — so the process's own idle session blocks its own
  # DROP DATABASE (or kills it via statement_timeout), permanently wedging
  # db:test:prepare (issue #409).
  #
  # Intercepting DatabaseTasks (rather than enhancing the rake tasks) covers
  # every route to a purge/drop: db:test:purge / db:purge / db:drop and their
  # per-database variants, maintain_test_schema!'s in-process purge, and
  # parallel-testing's TestDatabases — they all funnel through these methods.
  #
  # Installed only in processes that booted the app; a bare `db:drop` process
  # that never ran initializers has no BusRecord pool to block on anyway.
  module DatabaseTasksGuard
    def purge(...)
      Pgbus::BusRecord.disconnect_all_pools!
      super
    end

    def drop(...)
      Pgbus::BusRecord.disconnect_all_pools!
      super
    end
  end
end
