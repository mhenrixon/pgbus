# frozen_string_literal: true

Rails.application.configure do
  config.eager_load = false
  config.consider_all_requests_local = true
  config.action_dispatch.show_exceptions = :rescuable
  config.active_support.deprecation = :stderr

  # Ensure secrets are available for session/CSRF
  config.secret_key_base = "test_secret_key_base_for_pgbus_system_tests"
end
