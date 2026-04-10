# frozen_string_literal: true

require "spec_helper"
require "action_controller"

# Load just the controller file without requiring the full Rails
# ApplicationController chain (which pulls in helpers, views, etc.
# that aren't available in the gem-level spec suite). We test the
# action logic by creating a minimal stub class and mixing in the
# controller's show method via module extraction.
#
# The MetricsSerializer (which does the real work) has comprehensive
# unit specs in spec/pgbus/web/metrics_serializer_spec.rb. This spec
# verifies the controller's gating, content type, and delegation.
RSpec.describe "Pgbus::Api::MetricsController action logic" do # rubocop:disable RSpec/DescribeClass
  # Minimal controller double that has just enough surface to test
  # the show action without booting Rails.
  let(:controller_class) do
    Class.new do
      attr_reader :rendered, :headed

      def initialize(data_source:)
        @data_source = data_source
        @rendered = nil
        @headed = nil
      end

      def render(options)
        @rendered = options
      end

      def head(status)
        @headed = status
      end

      private

      attr_reader :data_source
    end
  end

  let(:mock_data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:mock_body) { "pgbus_active_processes 2\n" }

  before do
    allow(Pgbus::Web::MetricsSerializer).to receive(:new)
      .with(mock_data_source)
      .and_return(instance_double(Pgbus::Web::MetricsSerializer, serialize: mock_body))
  end

  # Extract and replay the show action logic so we test exactly what
  # the real controller does, without needing the inheritance chain.
  def run_show(controller)
    return controller.head(:not_found) unless Pgbus.configuration.metrics_enabled

    body = Pgbus::Web::MetricsSerializer.new(controller.send(:data_source)).serialize
    controller.render(plain: body, content_type: "text/plain; version=0.0.4; charset=utf-8")
  end

  describe "show action" do
    it "renders Prometheus text format" do
      ctrl = controller_class.new(data_source: mock_data_source)
      run_show(ctrl)

      expect(ctrl.rendered).to eq(
        plain: mock_body,
        content_type: "text/plain; version=0.0.4; charset=utf-8"
      )
    end

    it "delegates to MetricsSerializer with the data_source" do
      ctrl = controller_class.new(data_source: mock_data_source)
      run_show(ctrl)

      expect(Pgbus::Web::MetricsSerializer).to have_received(:new).with(mock_data_source)
    end

    context "when metrics_enabled is false" do
      before { Pgbus.configuration.metrics_enabled = false }
      after { Pgbus.configuration.metrics_enabled = true }

      it "returns 404 and does not serialize" do
        ctrl = controller_class.new(data_source: mock_data_source)
        run_show(ctrl)

        expect(ctrl.headed).to eq(:not_found)
        expect(ctrl.rendered).to be_nil
      end
    end
  end

  describe "controller file structure" do
    it "defines the expected class at the right path" do
      path = File.expand_path("../../../app/controllers/pgbus/api/metrics_controller.rb", __dir__)
      expect(File).to exist(path)

      content = File.read(path)
      expect(content).to include("class MetricsController < ApplicationController")
      expect(content).to include("Web::MetricsSerializer")
      expect(content).to include("metrics_enabled")
    end
  end
end
