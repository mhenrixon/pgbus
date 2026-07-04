# frozen_string_literal: true

# Shared setup for dashboard/API request specs. Injects the in-memory
# StubDataSource so no database is needed, and resets every piece of global
# Pgbus config a request spec can touch so the suite stays green in random
# order (no leakage between examples).
RSpec.configure do |config|
  config.before(:each, type: :request) do
    @stub_data_source = Pgbus::Test::StubDataSource.new

    Pgbus.configure do |c|
      c.web_live_updates = false
      c.web_refresh_interval = 0
      c.web_data_source = @stub_data_source
    end

    # The dummy app doesn't load framework defaults, so forgery protection is
    # on and every POST would 422 without a CSRF token. Request specs exercise
    # controller behavior, not the CSRF middleware, so disable it here and
    # restore it after each example.
    @forgery_protection = ActionController::Base.allow_forgery_protection
    ActionController::Base.allow_forgery_protection = false

    # Rails::Rack::Logger calls push_tags on Rails.logger's formatter for every
    # request. Some unit specs replace that formatter with a bare Pgbus
    # formatter (Configuration#log_format=) that lacks push_tags, which breaks
    # the request path. If the formatter has been swapped for one that can't
    # tag, heal it with a TaggedLogging formatter and restore afterwards so
    # request specs are green regardless of order.
    @rails_log_formatter = Rails.logger.formatter
    unless @rails_log_formatter.respond_to?(:push_tags)
      healed = Logger::Formatter.new
      healed.extend(ActiveSupport::TaggedLogging::Formatter)
      Rails.logger.formatter = healed
    end
  end

  config.after(:each, type: :request) do
    Rails.logger.formatter = @rails_log_formatter
    ActionController::Base.allow_forgery_protection = @forgery_protection

    Pgbus.configure do |c|
      c.web_auth = nil
      c.web_data_source = nil
      c.metrics_enabled = true
    end
    # authenticate_pgbus! warns once per process via this flag; reset it so the
    # "logs a warning" example is deterministic regardless of ordering.
    Pgbus::Web::Authentication.auth_warned = false
  end
end
