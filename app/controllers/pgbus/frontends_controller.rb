# frozen_string_literal: true

module Pgbus
  class FrontendsController < ActionController::Base
    protect_from_forgery with: :exception
    skip_after_action :verify_same_origin_request, raise: false

    FRONTEND_ROOT = Engine.root.join("app", "frontend", "pgbus")

    STATIC_ASSETS = {
      css: {
        style: FRONTEND_ROOT.join("style.css")
      },
      js: {
        apexcharts: FRONTEND_ROOT.join("vendor", "apexcharts.js")
      }
    }.freeze

    MODULE_OVERRIDES = {
      application: FRONTEND_ROOT.join("application.js"),
      turbo: FRONTEND_ROOT.join("vendor", "turbo.js")
    }.freeze

    def self.js_modules
      @js_modules ||= FRONTEND_ROOT.join("modules").children.select(&:file?)
                                   .to_h { |f| [File.basename(f.basename.to_s, ".js").to_sym, f] }
                                   .merge(MODULE_OVERRIDES)
    end

    CURRENT_VERSION = Pgbus::VERSION.tr(".", "-").freeze

    before_action do
      expires_in 1.year, public: true
    end

    after_action :set_asset_security_headers

    def static
      validate_version!
      file = STATIC_ASSETS.dig(params[:format]&.to_sym, params[:id]&.to_sym)
      raise ActionController::RoutingError, "Not Found" unless file&.exist?

      render file: file
    end

    def module
      validate_version!
      raise ActionController::RoutingError, "Not Found" if params[:format] != "js"

      file = self.class.js_modules[params[:id]&.to_sym]
      raise ActionController::RoutingError, "Not Found" unless file&.exist?

      render file: file
    end

    private

    def validate_version!
      return if params[:version] == CURRENT_VERSION

      raise ActionController::RoutingError, "Not Found"
    end

    def set_asset_security_headers
      response.headers["X-Content-Type-Options"] = "nosniff"
    end
  end
end
