# frozen_string_literal: true

require "json"

module Pgbus
  module Web
    module PayloadFilter
      module_function

      DEFAULT_FILTER_PATTERNS = [
        :password, :passphrase, :secret, :token, :authorization,
        :api_key, :private_key, :access_key, :secret_key,
        /(_|-)key$/
      ].freeze

      FILTERED = "[FILTERED]"

      def filter(value)
        return value unless Pgbus.configuration.dashboard_filter_sensitive

        case value
        when Hash
          parameter_filter.filter(value)
        when Array
          value.map { |element| filter(element) }
        else
          value
        end
      end

      def filter_json(value)
        return value unless Pgbus.configuration.dashboard_filter_sensitive

        case value
        when nil then nil
        when Hash
          JSON.generate(filter(value))
        when String
          parsed = JSON.parse(value)
          JSON.generate(filter(parsed))
        else
          value
        end
      rescue JSON::ParserError
        value
      end

      def parameter_filter
        patterns = Pgbus.configuration.dashboard_filter_parameters ||
                   rails_filter_parameters ||
                   DEFAULT_FILTER_PATTERNS
        ActiveSupport::ParameterFilter.new(patterns)
      end

      def rails_filter_parameters
        return unless defined?(Rails) &&
                      Rails.respond_to?(:application) &&
                      Rails.application.respond_to?(:config) &&
                      Rails.application.config.respond_to?(:filter_parameters)

        params = Rails.application.config.filter_parameters
        params if params.is_a?(Array) && params.any?
      end

      private_class_method :parameter_filter, :rails_filter_parameters
    end
  end
end
