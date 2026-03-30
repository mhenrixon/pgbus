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
        return if auth_block.nil?

        return if auth_block.respond_to?(:call) && auth_block.call(request)

        head :unauthorized
      end
    end
  end
end
