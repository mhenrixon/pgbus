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

      def load_all(paths, env: nil)
        normalized = Array(paths).compact.map { |p| p.respond_to?(:to_path) ? p.to_path : p.to_s }.reject(&:empty?)
        return {} if normalized.empty?

        env ||= detect_env

        normalized.each_with_object({}) do |path, acc|
          unless File.exist?(path.to_s)
            Pgbus.logger.warn { "[Pgbus] Recurring file not found, skipping: #{path}" }
            next
          end

          parsed = load(path, env: env)
          unless parsed.is_a?(Hash)
            Pgbus.logger.error { "[Pgbus] Invalid recurring config in #{path}: expected Hash, got #{parsed.class}" }
            next
          end
          parsed.each_key do |key|
            Pgbus.logger.debug { "[Pgbus] Recurring task '#{key}' overridden by #{path}" } if acc.key?(key)
          end
          acc.merge!(parsed)
        end
      end

      def detect_env
        if defined?(Rails) && Rails.respond_to?(:env) && Rails.env
          Rails.env.to_s
        else
          ENV.fetch("PGBUS_ENV", "development")
        end
      end
    end
  end
end
