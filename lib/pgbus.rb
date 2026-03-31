# frozen_string_literal: true

require "zeitwerk"

module Pgbus
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class SerializationError < Error; end
  class QueueNotFoundError < Error; end
  class DeadLetterError < Error; end
  class ConcurrencyLimitExceeded < Error; end

  class << self
    def loader
      @loader ||= begin
        loader = Zeitwerk::Loader.for_gem
        loader.inflector.inflect("pgbus" => "Pgbus", "cli" => "CLI", "dsl" => "DSL")
        loader.ignore("#{__dir__}/generators")
        loader.ignore("#{__dir__}/active_job")
        # Register app/models for non-Rails usage (specs, standalone).
        # When Rails is running, the Engine handles autoloading app/models.
        unless defined?(Rails::Engine)
          models_dir = File.expand_path("../app/models", __dir__)
          loader.push_dir(models_dir) if File.directory?(models_dir)
        end
        loader
      end
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

    def logger
      configuration.logger
    end
  end

  loader.setup
end

require "active_job/queue_adapters/pgbus_adapter" if defined?(ActiveJob)
require "pgbus/engine" if defined?(Rails::Engine)
