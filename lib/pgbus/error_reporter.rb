# frozen_string_literal: true

module Pgbus
  # Central error reporting module. Iterates all configured error reporters
  # and logs the error. Inspired by Sidekiq's error_handlers pattern.
  #
  # Usage:
  #   Pgbus::ErrorReporter.report(exception, { queue: "default" })
  #
  # Configuration:
  #   Pgbus.configure do |c|
  #     c.error_reporters << ->(ex, ctx) { Appsignal.set_error(ex) { |t| t.set_tags(ctx) } }
  #   end
  module ErrorReporter
    module_function

    def report(exception, context = {}, config: Pgbus.configuration)
      log_error(exception, context, config: config)

      config.error_reporters.each do |handler|
        call_handler(handler, exception, context, config)
      rescue Exception => e # rubocop:disable Lint/RescueException
        config.logger.error { "[Pgbus] Error reporter raised: #{e.class}: #{e.message}" }
      end
    rescue Exception # rubocop:disable Lint/RescueException
      # ErrorReporter must never raise — callers sit inside rescue blocks
      # where an unexpected raise would break fault-tolerance invariants.
      nil
    end

    def call_handler(handler, exception, context, config)
      target = handler.is_a?(Proc) ? handler : handler.method(:call)
      if target.arity == 3 || (target.arity.negative? && target.parameters.size >= 3)
        handler.call(exception, context, config)
      else
        handler.call(exception, context)
      end
    end

    def log_error(exception, context, config:)
      config.logger.error do
        msg = "[Pgbus] #{exception.class}: #{exception.message}"
        msg += " (#{context.inspect})" unless context.empty?
        msg
      end
    end
  end
end
