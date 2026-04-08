# frozen_string_literal: true

require "zeitwerk"

module Pgbus
  # Suffix appended to a queue name to derive its dead-letter companion
  # (e.g. "pgbus_default" -> "pgbus_default_dlq"). Hard-coded here because
  # changing it on a running deployment would orphan every existing DLQ
  # message; nothing in the codebase or in user reports has ever needed
  # this to be configurable.
  DEAD_LETTER_SUFFIX = "_dlq"

  class Error < StandardError; end
  class ConfigurationError < Error; end
  class SerializationError < Error; end
  class QueueNotFoundError < Error; end
  class DeadLetterError < Error; end
  class ConcurrencyLimitExceeded < Error; end
  class JobNotUnique < Error; end
  class SchemaNotReady < Error; end

  class << self
    # Process-global flag set by Worker#graceful_shutdown so the adapter
    # can report stopping? to ActiveJob::Continuation (Rails 8.1+).
    attr_writer :stopping

    def stopping
      @stopping || false
    end

    def loader
      @loader ||= begin
        loader = Zeitwerk::Loader.for_gem
        loader.inflector.inflect(
          "pgbus" => "Pgbus",
          "cli" => "CLI",
          "dsl" => "DSL",
          "capsule_dsl" => "CapsuleDSL"
        )
        loader.ignore("#{__dir__}/generators")
        loader.ignore("#{__dir__}/active_job")
        # lib/puma/plugin/pgbus_streams.rb is a Puma plugin — it's required
        # explicitly by the user from config/puma.rb via `plugin :pgbus_streams`.
        # Without this ignore, Zeitwerk scans lib/puma/ under the pgbus loader
        # root and tries to autoload Puma::Plugin, which collides with the real
        # Puma::Plugin class defined by the puma gem itself.
        loader.ignore("#{__dir__}/puma")
        loader
      end
    end

    # Separate loader for app/models used only in non-Rails contexts (specs,
    # standalone scripts). When the Engine boots, Rails' autoloader takes over
    # app/models and this loader is torn down to avoid conflicts.
    def models_loader
      models_dir = File.expand_path("../app/models", __dir__)
      return nil unless File.directory?(models_dir)

      @models_loader ||= begin
        loader = Zeitwerk::Loader.new
        loader.tag = "pgbus-models"
        loader.push_dir(models_dir)
        loader.setup
        loader
      end
    end

    def teardown_models_loader!
      return unless @models_loader

      @models_loader.unregister
      @models_loader = nil
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield configuration
    end

    def client
      @client ||= Client.new(configuration)
    end

    def reset!
      @client&.close
      @client = nil
      @configuration = nil
    end

    # Discard the inherited PGMQ client after fork.
    # Do NOT call close — the parent's @pgmq_mutex is in undefined
    # state post-fork and attempting to acquire it can deadlock.
    # The next call to Pgbus.client will lazily create a fresh one.
    def reset_client!
      @client = nil
    end

    def logger
      configuration.logger
    end

    def logger=(value)
      configuration.logger = value
    end
  end

  loader.setup

  # In non-Rails contexts, set up model autoloading via a separate loader.
  # This is torn down by the Engine initializer when Rails boots.
  models_loader unless defined?(Rails::Engine)
end

require "active_job/queue_adapters/pgbus_adapter" if defined?(ActiveJob)
require "pgbus/engine" if defined?(Rails::Engine)
