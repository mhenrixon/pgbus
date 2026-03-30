# frozen_string_literal: true

module Pgbus
  class DashboardController < ApplicationController
    def show
      @stats = data_source.summary_stats
      @queues = data_source.queues_with_metrics
      @processes = data_source.processes
      @recent_failures = data_source.failed_events(page: 1, per_page: 5)
    end
  end
end
