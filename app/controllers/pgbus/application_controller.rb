# frozen_string_literal: true

module Pgbus
  class ApplicationController < ActionController::Base
    include Web::Authentication

    protect_from_forgery with: :exception

    layout "pgbus/application"

    helper Pgbus::ApplicationHelper

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
