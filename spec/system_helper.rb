# frozen_string_literal: true

# Boot Rails dummy app first — this ensures the engine registers properly
ENV["RAILS_ENV"] = "test"
require_relative "dummy/config/environment"

require "rspec/rails"
require "capybara/rspec"

RSpec.configure do |config|
  config.infer_spec_type_from_file_location!

  config.before(:suite) do
    Pgbus.configure do |c|
      c.logger = Logger.new(IO::NULL)
    end
  end
end

Dir[File.join(__dir__, "system/support/**/*.rb")].each { |f| require f }
