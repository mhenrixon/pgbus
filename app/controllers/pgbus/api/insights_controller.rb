# frozen_string_literal: true

module Pgbus
  module Api
    class InsightsController < ApplicationController
      def show
        minutes = (params[:minutes] || Pgbus.configuration.insights_default_minutes).to_i
        render json: {
          summary: data_source.job_stats_summary(minutes: minutes),
          throughput: data_source.job_throughput(minutes: minutes),
          status_counts: data_source.job_status_counts(minutes: minutes),
          slowest: data_source.slowest_job_classes(minutes: minutes)
        }
      end
    end
  end
end
