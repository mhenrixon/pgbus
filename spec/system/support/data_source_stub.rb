# frozen_string_literal: true

# The stub class lives under spec/support/ so request specs (loaded via
# spec_helper) share it. system_helper does not require spec_helper, so load
# the class explicitly here for system specs.
require_relative "../../support/pgbus/stub_data_source"

RSpec.configure do |config|
  config.before(:each, type: :system) do
    @stub_data_source = Pgbus::Test::StubDataSource.new

    Pgbus.configure do |c|
      c.web_live_updates = false
      c.web_refresh_interval = 0
      c.web_data_source = @stub_data_source
    end
  end

  config.after(:each, type: :system) do
    Pgbus.configure do |c|
      c.web_data_source = nil
    end
  end
end
