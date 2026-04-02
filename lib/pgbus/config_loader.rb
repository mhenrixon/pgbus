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
      config_hash = parsed.fetch(env, parsed)
      apply(config_hash)
    end

    def apply(hash)
      config = Pgbus.configuration
      hash.each do |key, value|
        setter = :"#{key}="
        config.public_send(setter, value) if config.respond_to?(setter)
      end
      config
    end
  end
end
