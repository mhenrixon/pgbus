# frozen_string_literal: true

module Pgbus
  module Web
    module Authentication
      extend ActiveSupport::Concern

      included do
        before_action :authenticate_pgbus!
      end

      class << self
        attr_accessor :auth_warned
      end

      # Default base controller — anything else means the app deliberately
      # chose a controller (e.g. an AdminController) that already gates access,
      # so the dashboard isn't actually open and the warning is a false positive.
      DEFAULT_BASE_CONTROLLER = "::ActionController::Base"
      private_constant :DEFAULT_BASE_CONTROLLER

      private

      def authenticate_pgbus!
        auth_block = Pgbus.configuration.web_auth

        if auth_block.nil?
          warn_unauthenticated_dashboard
          return
        end

        return if auth_block.respond_to?(:call) && auth_block.call(request)

        head :unauthorized
      end

      def warn_unauthenticated_dashboard
        return if Pgbus::Web::Authentication.auth_warned
        # A non-default base_controller_class signals host-level auth is in
        # place; don't nag apps that gate the dashboard via a custom controller
        # (they had to set a redundant web_auth lambda just to silence us — #334).
        return unless Pgbus.configuration.base_controller_class.to_s == DEFAULT_BASE_CONTROLLER

        Pgbus.logger.warn do
          "[Pgbus] Dashboard is accessible without authentication. " \
            "Configure Pgbus.configuration.web_auth to restrict access. " \
            "See: https://github.com/mhenrixon/pgbus#dashboard-authentication"
        end
        Pgbus::Web::Authentication.auth_warned = true
      end
    end
  end
end
