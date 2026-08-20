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

    # Disconnects every pool owned by this class — the pools `connects_to`
    # creates when pgbus runs on a dedicated database — across all roles.
    # When pgbus runs in the primary database there is no BusRecord-owned
    # pool and this is a no-op; ActiveRecord::Base's pool is never touched.
    #
    # Called by Pgbus::DatabaseTasksGuard before every database purge/drop so
    # an idle boot-time session on the pgbus database can never block that
    # same process's DROP DATABASE (issue #409). Safe to call at any time:
    # the next checkout after a disconnect reconnects transparently.
    def self.disconnect_all_pools!
      connection_handler.connection_pool_list(:all).each do |pool|
        pool.disconnect! if owns_pool?(pool)
      end
    end

    # Rails 8.0 replaced ConnectionPool#connection_class with
    # #connection_descriptor — a ConnectionDescriptor whose #name is the
    # owning class name (issue #411). Pools `connects_to` creates register
    # under the class name, so the name comparison is equivalent to the
    # class-identity check on 7.x. A nil descriptor (null pool) never matches.
    def self.owns_pool?(pool)
      if pool.respond_to?(:connection_descriptor)
        pool.connection_descriptor&.name == name
      else
        pool.connection_class == self
      end
    end
    private_class_method :owns_pool?
  end
end
