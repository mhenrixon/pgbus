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
      next if Pgbus.configuration.recurring_tasks

      config = Pgbus.configuration
      files = config.recurring_tasks_files
      default_path = app.root.join("config", "recurring.yml")

      if files
        tasks = Pgbus::Recurring::ConfigLoader.load_all(files)
        if tasks.empty? && default_path.exist? && files.none? { |f| File.expand_path(f) == File.expand_path(default_path) }
          tasks = Pgbus::Recurring::ConfigLoader.load(default_path)
          config.recurring_tasks_file ||= default_path.to_s
        end
        config.recurring_tasks = tasks unless tasks.empty?
      elsif default_path.exist?
        config.recurring_tasks = Pgbus::Recurring::ConfigLoader.load(default_path)
        config.recurring_tasks_file ||= default_path.to_s
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
      load File.expand_path("../tasks/pgbus_streams.rake", __dir__)
      load File.expand_path("../tasks/pgbus_autovacuum.rake", __dir__)
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

    # Make stream_source_element.js available to the host app's asset
    # pipeline (Propshaft or Sprockets) so it can be included via
    # `javascript_include_tag "pgbus/stream_source_element"` or pinned
    # in importmap. When importmap-rails is loaded, auto-pin it so
    # host apps get it without manual configuration.
    initializer "pgbus.streams.assets" do |app|
      if Pgbus.configuration.streams_enabled && app.config.respond_to?(:assets)
        app.config.assets.precompile += %w[pgbus/stream_source_element.js]
      end
    end

    initializer "pgbus.streams.importmap" do
      if Pgbus.configuration.streams_enabled && defined?(::Importmap::Map)
        ActiveSupport.on_load(:after_initialize) do
          next unless Rails.application.respond_to?(:importmap)

          Rails.application.importmap.pin(
            "pgbus/stream_source_element", to: "pgbus/stream_source_element.js"
          )
        end
      end
    end

    # Install the Turbo::StreamsChannel patch after turbo-rails has been
    # loaded. The patch redirects broadcast_stream_to through Pgbus.stream
    # instead of ActionCable. When turbo-rails is not loaded, this is a
    # no-op and pgbus_stream_from still works via the explicit
    # Pgbus.stream(...).broadcast(...) API.
    initializer "pgbus.streams.turbo_broadcastable", after: :load_config_initializers do
      ActiveSupport.on_load(:after_initialize) do
        if Pgbus.configuration.streams_enabled
          # Touch the constant first so Zeitwerk autoloads
          # lib/pgbus/streams/turbo_broadcastable.rb. The file defines
          # `Pgbus::Streams::TurboBroadcastable` (the autoloaded const)
          # AND `Pgbus::Streams.install_turbo_broadcastable_patch!`
          # (a side-effect class method on the parent module). Without
          # the constant reference, Zeitwerk doesn't load the file and
          # the method call below raises NoMethodError. Assigning to
          # `_` keeps RuboCop's Lint/Void from deleting the line.
          _autoload_trigger = Pgbus::Streams::TurboBroadcastable
          Pgbus::Streams.install_turbo_broadcastable_patch!

          # Subscribe-side patch: override turbo_stream_from to render
          # <pgbus-stream-source> (SSE) instead of <turbo-cable-stream-source>
          # (ActionCable). Without this, third-party gems like
          # hotwire-livereload that call turbo_stream_from in their views
          # subscribe via ActionCable while the broadcast (patched above)
          # goes through PGMQ — the message never arrives.
          _autoload_trigger_override = Pgbus::Streams::TurboStreamOverride
          Pgbus::Streams.install_turbo_stream_override!
        end
      end
    end
  end
end
