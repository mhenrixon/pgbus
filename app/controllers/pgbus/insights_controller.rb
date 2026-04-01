# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @summary = data_source.job_stats_summary
      @slowest = data_source.slowest_job_classes
    end
  end
end
