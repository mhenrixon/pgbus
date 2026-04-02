# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @minutes = (params[:minutes] || Pgbus.configuration.insights_default_minutes).to_i
      @summary = data_source.job_stats_summary(minutes: @minutes)
      @slowest = data_source.slowest_job_classes(minutes: @minutes)
    end
  end
end
