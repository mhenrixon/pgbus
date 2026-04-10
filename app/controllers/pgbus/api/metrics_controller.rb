# frozen_string_literal: true

module Pgbus
  module Api
    class MetricsController < ApplicationController
      PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

      def show
        return head(:not_found) unless Pgbus.configuration.metrics_enabled

        body = Web::MetricsSerializer.new(data_source).serialize
        render plain: body, content_type: PROMETHEUS_CONTENT_TYPE
      end
    end
  end
end
