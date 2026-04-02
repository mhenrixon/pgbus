# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @minutes = insights_minutes
      @summary = data_source.job_stats_summary(minutes: @minutes)
      @slowest = data_source.slowest_job_classes(minutes: @minutes)
    end
  end
end
