# frozen_string_literal: true

module Pgbus
  module Metrics
    # Rack app that renders the in-process Prometheus registry as text exposition
    # format. Mount it in the host Rails app (or any Rack server) so Prometheus
    # can scrape it — see the README. Mirrors the "mountable Rack app" shape used
    # by Pgbus::Web::StreamApp.
    #
    #   # config/routes.rb
    #   mount Pgbus::Metrics::PrometheusExporter.new => "/metrics"
    #
    # With no injected backend it reads Pgbus.configuration.metrics_backend, so a
    # single `config.metrics_backend = :prometheus` wires both the subscriber and
    # the exporter to the same registry.
    class PrometheusExporter
      CONTENT_TYPE = "text/plain; version=0.0.4"

      def initialize(backend: nil)
        @backend = backend
      end

      def call(_env)
        backend = @backend || Pgbus.configuration.metrics_backend
        unless backend.is_a?(Backends::Prometheus)
          return [503, { "content-type" => "text/plain" },
                  ["metrics_backend is not a Prometheus backend\n"]]
        end

        [200, { "content-type" => CONTENT_TYPE }, [backend.render]]
      end
    end
  end
end
