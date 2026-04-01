# frozen_string_literal: true

require_relative "boot"

require "rails"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"

Bundler.require(*Rails.groups)
require "pgbus"
# Ensure the engine is loaded even if pgbus was required before Rails
# (e.g. when unit specs using spec_helper run before system specs).
require "pgbus/engine"

module Dummy
  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.to_f
    config.eager_load = false
    config.hosts.clear
  end
end
