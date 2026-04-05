# frozen_string_literal: true

Rails.application.configure do
  config.eager_load = false
  config.consider_all_requests_local = true
  config.active_support.deprecation = :log
  config.secret_key_base = "dev_secret_key_base_for_pgbus_dummy"
end
