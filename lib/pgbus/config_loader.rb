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
      # Distinguish sectioned (top-level env keys mapping to Hashes) from
      # flat (top-level setter keys mapping to scalars/arrays). parsed.key?(env)
      # alone can't tell them apart, so a flat file silently lost its typo
      # warnings whenever no env section happened to match its keys.
      if sectioned?(parsed)
        apply(parsed.fetch(env)) if parsed.key?(env)
      else
        apply(parsed)
      end
    end

    def sectioned?(parsed)
      parsed.is_a?(Hash) && parsed.any? && parsed.values.all?(Hash)
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
      # Validate eagerly so a bad YAML value aborts boot with an ArgumentError
      # naming the offending key, instead of failing later in a worker path.
      # Honors an `eager_validation: false` key in the same hash (applied above).
      config.validate! if config.eager_validation
      config
    end
  end
end
