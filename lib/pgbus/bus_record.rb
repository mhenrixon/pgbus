# frozen_string_literal: true

require "active_record"

module Pgbus
  # Base class for all Pgbus ActiveRecord models.
  #
  # Lives in lib/pgbus/ so the main Zeitwerk gem loader picks it up
  # regardless of Rails engine boot order. This avoids the NameError
  # that occurs when a host app uses selective railtie requires
  # (require "rails" + individual railties instead of require "rails/all")
  # and the engine's app/models path isn't registered yet.
  class BusRecord < ActiveRecord::Base
    self.abstract_class = true
  end
end
