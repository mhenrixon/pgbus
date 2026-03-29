# frozen_string_literal: true

module Pgbus
  class ApplicationController < ActionController::Base
    include Web::Authentication

    layout "pgbus/application"

    helper Pgbus::ApplicationHelper

    private

    def data_source
      @data_source ||= Web::DataSource.new
    end

    def page_param
      [params[:page].to_i, 1].max
    end

    def per_page
      Pgbus.configuration.web_per_page
    end
  end
end
