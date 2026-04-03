# frozen_string_literal: true

module Pgbus
  class OutboxEntry < BusRecord
    self.table_name = "pgbus_outbox_entries"

    scope :unpublished, -> { where(published_at: nil) }
    scope :published_before, ->(time) { where(published_at: ...time) }
  end
end
