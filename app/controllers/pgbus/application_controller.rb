# frozen_string_literal: true

module Pgbus
  class ApplicationController < ActionController::Base
    include Web::Authentication

    protect_from_forgery with: :exception

    layout "pgbus/application"

    helper Pgbus::ApplicationHelper

    # Make `pgbus` route proxy available in views (e.g. pgbus.root_path).
    # With isolate_namespace, the non-prefixed helpers (root_path) work inside
    # the engine, but the views use the pgbus.* proxy form for clarity.
    def pgbus
      @pgbus ||= Pgbus::Engine.routes.url_helpers
    end
    helper_method :pgbus

    private

    def data_source
      @data_source ||= Pgbus.configuration.web_data_source || Web::DataSource.new
    end

    def page_param
      [params[:page].to_i, 1].max
    end

    def per_page
      Pgbus.configuration.web_per_page
    end

    def turbo_frame_request?
      request.headers["Turbo-Frame"].present? || params[:frame].present?
    end

    def render_frame(partial)
      render partial: partial, layout: false
    end
  end
end
