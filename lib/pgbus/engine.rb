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
        Pgbus::BusRecord.connects_to(**Pgbus.configuration.connects_to) if Pgbus.configuration.connects_to
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

    initializer "pgbus.i18n" do
      config.i18n.load_path += Dir[root.join("config", "locales", "**", "*.yml")]
    end

    initializer "pgbus.web" do
      require "pgbus/web/authentication"
      require "pgbus/web/data_source"
    end

    # Install the watermark cache middleware ahead of the app's own
    # middleware so the thread-local cache is cleared between every
    # Rack request. Without this, repeated page renders served by the
    # same Puma thread would see stale current_msg_id values.
    initializer "pgbus.streams.middleware" do |app|
      app.middleware.use Pgbus::Streams::WatermarkCacheMiddleware if Pgbus.configuration.streams_enabled
    end

    # Install the Turbo::StreamsChannel patch after turbo-rails has been
    # loaded. The patch redirects broadcast_stream_to through Pgbus.stream
    # instead of ActionCable. When turbo-rails is not loaded, this is a
    # no-op and pgbus_stream_from still works via the explicit
    # Pgbus.stream(...).broadcast(...) API.
    initializer "pgbus.streams.turbo_broadcastable", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) do
        Pgbus::Streams.install_turbo_broadcastable_patch! if Pgbus.configuration.streams_enabled
      end
    end
  end
end
