# frozen_string_literal: true

require "yaml"
require "erb"

module Pgbus
  # Loads a legacy +config/pgbus.yml+ into the running +Pgbus.configuration+.
  #
  # DEPRECATED (upgrade path only). Since #317, +pgbus:install+ generates a Ruby
  # initializer (+config/initializers/pgbus.rb+) instead of YAML, and the Ruby
  # DSL is the supported config surface. This loader stays for apps that predate
  # that change and still ship a +config/pgbus.yml+; +pgbus:update+ converts such
  # a file to an initializer. YAML config support is slated for removal in 2.0.
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
        if parsed.key?(env)
          apply(parsed.fetch(env))
        else
          warn_missing_env_section(parsed, env)
        end
      else
        apply(parsed)
      end
    end

    # A file is "sectioned" (top-level keys are env names, e.g. development:/
    # production:) rather than "flat" (top-level keys are Configuration
    # setters) when every top-level value is a Hash AND none of the
    # top-level keys is itself a real Configuration setter. The setter check
    # is what distinguishes the two: env section names (production, staging,
    # ...) are never real setters, so a legitimate flat file whose settings
    # all happen to be Hash-valued (connection_params:, streams_retention:,
    # recurring_tasks:, ...) is correctly classified as flat instead of being
    # misread as sectioned-with-no-matching-env and silently skipped.
    def sectioned?(parsed)
      return false unless parsed.is_a?(Hash) && parsed.any? && parsed.values.all?(Hash)

      config = Pgbus.configuration
      parsed.keys.none? { |key| config.respond_to?(:"#{key}=") }
    end

    # A sectioned file with no section for the current env has nothing to
    # apply for this process — that's a real gap (typo'd env name, or a
    # config file that just doesn't cover this deployment), so it must be
    # loud instead of a silent no-op.
    def warn_missing_env_section(parsed, env)
      Pgbus.configuration.logger.warn do
        "[Pgbus] No configuration section for env #{env.inspect} — " \
          "available sections: #{parsed.keys.inspect}. Nothing was applied."
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
      # Validate eagerly so a bad YAML value aborts boot with an ArgumentError
      # naming the offending key, instead of failing later in a worker path.
      # Honors an `eager_validation: false` key in the same hash (applied above).
      config.validate! if config.eager_validation
      config
    end
  end
end
