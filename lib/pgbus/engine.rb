# frozen_string_literal: true

require "rails/engine"

module Pgbus
  class Engine < ::Rails::Engine
    isolate_namespace Pgbus

    initializer "pgbus.configure" do |app|
      config_path = app.root.join("config", "pgbus.yml")
      Pgbus::ConfigLoader.load(config_path) if config_path.exist?
    end

    initializer "pgbus.recurring" do |app|
      recurring_path = app.root.join("config", "recurring.yml")
      if recurring_path.exist? && !Pgbus.configuration.recurring_tasks
        Pgbus.configuration.recurring_tasks = Pgbus::Recurring::ConfigLoader.load(recurring_path)
        Pgbus.configuration.recurring_tasks_file ||= recurring_path.to_s
      end
    end

    initializer "pgbus.active_job" do
      ActiveSupport.on_load(:active_job) do
        include Pgbus::Concurrency
      end
    end

    initializer "pgbus.logger" do
      ActiveSupport.on_load(:after_initialize) do
        Pgbus.configuration.logger ||= Rails.logger
      end
    end

    rake_tasks do
      load File.expand_path("../tasks/pgbus_pgmq.rake", __dir__)
    end

    initializer "pgbus.web" do
      require "pgbus/web/authentication"
      require "pgbus/web/data_source"
    end
  end
end
