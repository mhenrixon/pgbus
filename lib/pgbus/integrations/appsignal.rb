# frozen_string_literal: true

require "pgbus/integrations/appsignal/subscriber"
require "pgbus/integrations/appsignal/probe"

module Pgbus
  module Integrations
    # AppSignal integration for pgbus.
    #
    # Loaded automatically by Pgbus::Engine when the appsignal gem is present
    # and config.appsignal_enabled is true (default). To opt out:
    #
    #   Pgbus.configure do |c|
    #     c.appsignal_enabled = false
    #   end
    #
    # The integration:
    #   * Subscribes to pgbus.* ActiveSupport::Notifications and translates
    #     them into AppSignal background-job transactions and metrics.
    #   * Registers a minutely probe that reports queue depth, DLQ size,
    #     dead-tuple counts, MVCC horizon age, and stream stats from
    #     Pgbus::Web::DataSource.
    #
    # All metric names are prefixed `pgbus_` so they group cleanly in
    # AppSignal's custom-metrics view.
    module Appsignal
      module_function

      def install! # rubocop:disable Naming/PredicateMethod
        return false unless defined?(::Appsignal)
        return false if @installed

        Subscriber.install!
        Probe.install! if Pgbus.configuration.appsignal_probe_enabled
        @installed = true
        Pgbus.logger.info { "[Pgbus] AppSignal integration installed" }
        true
      end

      def installed?
        @installed == true
      end

      # Test hook: tear everything down so a fresh install! can run.
      def reset!
        Subscriber.reset! if defined?(Subscriber)
        Probe.reset! if defined?(Probe)
        @installed = false
      end
    end
  end
end
