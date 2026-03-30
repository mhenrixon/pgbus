# frozen_string_literal: true

RSpec.configure do |config|
  config.before(:each, type: :system) do
    Capybara.configure do |c|
      c.default_max_wait_time = 5
      c.default_driver = :playwright
      c.javascript_driver = :playwright
      c.save_path = "tmp/capybara"
      c.always_include_port = true
    end

    driven_by(:playwright, screen_size: [1280, 800])
  end
end
