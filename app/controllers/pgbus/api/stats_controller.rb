# frozen_string_literal: true

module Pgbus
  module Api
    class StatsController < ApplicationController
      def show
        render json: data_source.summary_stats
      end
    end
  end
end
