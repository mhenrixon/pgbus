# frozen_string_literal: true

require "rails/engine"

module Pgbus
  class Engine < ::Rails::Engine
    isolate_namespace Pgbus

    initializer "pgbus.configure" do |app|
      config_path = app.root.join("config", "pgbus.yml")
      if config_path.exist?
        Pgbus::ConfigLoader.load(config_path)
      end
    end

    initializer "pgbus.active_job" do
      ActiveSupport.on_load(:active_job) do
        require "pgbus/active_job/adapter"
      end
    end

    initializer "pgbus.logger" do
      ActiveSupport.on_load(:after_initialize) do
        Pgbus.configuration.logger ||= Rails.logger
      end
    end
  end
end
