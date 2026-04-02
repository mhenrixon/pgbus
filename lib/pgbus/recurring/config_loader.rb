# frozen_string_literal: true

require "yaml"
require "erb"

module Pgbus
  module Recurring
    module ConfigLoader
      module_function

      def load(path, env: nil)
        return {} unless path && File.exist?(path.to_s)

        env ||= detect_env
        raw = File.read(path)
        parsed = YAML.safe_load(ERB.new(raw).result, permitted_classes: [Symbol], aliases: true)
        return {} unless parsed.is_a?(Hash)

        # If the parsed hash has an environment key, use that subtree
        parsed.key?(env) ? parsed.fetch(env, {}) : parsed
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to load recurring config from #{path}: #{e.message}" }
        {}
      end

      def detect_env
        if defined?(Rails) && Rails.respond_to?(:env)
          Rails.env.to_s
        else
          ENV.fetch("PGBUS_ENV", "development")
        end
      end
    end
  end
end
