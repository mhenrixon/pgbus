# frozen_string_literal: true

module Pgbus
  class ProcessedEvent < BusRecord
    self.table_name = "pgbus_processed_events"

    scope :expired, ->(before) { where("processed_at < ?", before) }
  end
end
