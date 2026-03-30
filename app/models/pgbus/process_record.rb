# frozen_string_literal: true

module Pgbus
  class ProcessRecord < ApplicationRecord
    self.table_name = "pgbus_processes"

    scope :stale, ->(threshold) { where("last_heartbeat_at < ?", threshold) }
  end
end
