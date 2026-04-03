# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @minutes = insights_minutes
      @summary = data_source.job_stats_summary(minutes: @minutes)
      @slowest = data_source.slowest_job_classes(minutes: @minutes)
      @latency_by_queue = data_source.latency_by_queue(minutes: @minutes)
      @latency_available = Pgbus::JobStat.latency_columns?
    end
  end
end
