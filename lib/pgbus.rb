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

require "pgbus/engine" if defined?(Rails::Engine)
