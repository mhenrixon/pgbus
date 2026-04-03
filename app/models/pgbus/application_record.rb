# frozen_string_literal: true

module Pgbus
  # Backward-compatible alias — host apps that subclassed
  # Pgbus::ApplicationRecord will continue to work.
  # New code should inherit from Pgbus::BusRecord directly.
  class ApplicationRecord < BusRecord
    self.abstract_class = true
  end
end
