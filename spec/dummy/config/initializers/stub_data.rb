# frozen_string_literal: true

# Inject rich stub data for dashboard QA when PGBUS_STUB_DATA=1
if ENV["PGBUS_STUB_DATA"] == "1"
  Rails.application.config.after_initialize do
    require_relative "../../lib/stub_data_source"
    stub = DummyApp::StubDataSource.build
    Pgbus.configure do |c|
      c.web_data_source = stub
      c.web_refresh_interval = 5000
    end
    Rails.logger.info "[Pgbus Dummy] Stub data source loaded with sample data"
  end
end
