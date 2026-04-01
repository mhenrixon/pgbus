# frozen_string_literal: true

require "rails/engine"

module Pgbus
  class Engine < ::Rails::Engine
    isolate_namespace Pgbus

    # When pgbus was loaded before Rails, a separate Zeitwerk loader manages
    # app/models. Tear it down before Rails' autoloader claims the same paths.
    initializer "pgbus.release_models_loader", before: :set_autoload_paths do
      Pgbus.teardown_models_loader!
    end

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

    initializer "pgbus.db" do
      ActiveSupport.on_load(:active_record) do
        Pgbus::ApplicationRecord.connects_to(**Pgbus.configuration.connects_to) if Pgbus.configuration.connects_to
      end
    end

    initializer "pgbus.active_job" do
      ActiveSupport.on_load(:active_job) do
        include Pgbus::Concurrency
        include Pgbus::Uniqueness
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
