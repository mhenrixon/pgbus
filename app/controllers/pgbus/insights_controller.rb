# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @minutes = insights_minutes
      @summary = data_source.job_stats_summary(minutes: @minutes)
      @slowest = data_source.slowest_job_classes(minutes: @minutes)
      @latency_by_queue = data_source.latency_by_queue(minutes: @minutes)
      @latency_available = Pgbus::JobStat.latency_columns?

      @stream_stats_available = data_source.stream_stats_available?
      return unless @stream_stats_available

      @stream_summary = data_source.stream_stats_summary(minutes: @minutes)
      @top_streams = data_source.top_streams(minutes: @minutes)
    end
  end
end
