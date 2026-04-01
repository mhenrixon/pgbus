# frozen_string_literal: true

module Pgbus
  module Api
    class InsightsController < ApplicationController
      def show
        render json: {
          summary: data_source.job_stats_summary,
          throughput: data_source.job_throughput,
          status_counts: data_source.job_status_counts,
          slowest: data_source.slowest_job_classes
        }
      end
    end
  end
end
