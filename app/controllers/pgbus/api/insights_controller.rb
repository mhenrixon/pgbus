# frozen_string_literal: true

module Pgbus
  module Api
    class InsightsController < ApplicationController
      def show
        minutes = insights_minutes
        payload = {
          summary: data_source.job_stats_summary(minutes: minutes),
          throughput: data_source.job_throughput(minutes: minutes),
          status_counts: data_source.job_status_counts(minutes: minutes),
          slowest: data_source.slowest_job_classes(minutes: minutes)
        }
        if Pgbus::JobStat.latency_columns?
          payload[:latency_trend] = data_source.latency_trend(minutes: minutes)
          payload[:latency_by_queue] = data_source.latency_by_queue(minutes: minutes)
        end
        render json: payload
      end
    end
  end
end
