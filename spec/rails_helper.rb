# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"
require_relative "dummy/config/environment"

require "spec_helper"
require "rspec/rails"

RSpec.configure do |config|
  config.infer_spec_type_from_file_location!

  # Transactional fixtures open a process-wide ActiveRecord transaction around
  # EVERY example once rspec-rails is loaded — including pure unit specs that
  # only `require "spec_helper"`. That ambient open transaction makes
  # Pgbus::Streams::Stream#broadcast defer to `after_commit` (returning nil
  # instead of the msg_id), so streams unit specs fail whenever a rails_helper
  # spec happens to load first in a random-ordered run. No spec here relies on
  # DB fixtures (integration/system specs use their own helpers), so leave it
  # off by default and let a DB-backed example opt in with the
  # `:use_transactional_tests` metadata tag if one is ever added.
  config.use_transactional_fixtures = false
  config.before(:each, :use_transactional_tests) do
    self.use_transactional_tests = true
  end
end
