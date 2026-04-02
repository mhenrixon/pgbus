# frozen_string_literal: true

module Pgbus
  module Web
    module Authentication
      extend ActiveSupport::Concern

      included do
        before_action :authenticate_pgbus!
      end

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
        return if @_pgbus_auth_warned

        Pgbus.logger.warn do
          "[Pgbus] Dashboard is accessible without authentication. " \
            "Configure Pgbus.configuration.web_auth to restrict access. " \
            "See: https://github.com/mhenrixon/pgbus#dashboard-authentication"
        end
        @_pgbus_auth_warned = true
      end
    end
  end
end
