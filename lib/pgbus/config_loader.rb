# frozen_string_literal: true

require "yaml"
require "erb"

module Pgbus
  module ConfigLoader
    module_function

    def load(path, env: nil)
      env ||= (defined?(Rails) && Rails.respond_to?(:env) && Rails.env) || ENV.fetch("PGBUS_ENV", "development")
      raw = File.read(path)
      parsed = YAML.safe_load(ERB.new(raw).result, permitted_classes: [Symbol], aliases: true)
      if parsed.key?(env)
        apply(parsed.fetch(env))
      else
        # No section for this env — treat the file as flat (un-sectioned)
        # config. Suppress unknown-key warnings here: if the file IS
        # env-sectioned and this env just isn't in it, every other env name
        # ("production", "staging", ...) would be flagged as a typo.
        apply(parsed, warn_unknown: false)
      end
    end

    def apply(hash, warn_unknown: true)
      config = Pgbus.configuration
      hash.each do |key, value|
        setter = :"#{key}="
        if config.respond_to?(setter)
          config.public_send(setter, value)
        elsif warn_unknown
          config.logger.warn { "[Pgbus] Unknown configuration key ignored: #{key.inspect} — check for typos in pgbus.yml" }
        end
      end
      config
    end
  end
end
