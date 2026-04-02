# frozen_string_literal: true

require "active_record"
require "pgbus"
require "globalid"

# GlobalID requires an app name to parse/create GIDs
GlobalID.app = "pgbus-test"

Dir[File.join(__dir__, "support/**/*.rb")].each { |f| require f }

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.order = :random
  Kernel.srand config.seed

  config.before(:suite) do
    Pgbus.configure do |c|
      c.logger = Logger.new(IO::NULL)
      c.queue_prefix = "pgbus_test"
      c.default_queue = "default"
    end
  end

  config.after(:suite) do
    Pgbus.reset!
  end
end
