# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Vendored frontend assets" do # rubocop:disable RSpec/DescribeClass
  let(:frontend_dir) { Pgbus::Engine.root.join("app", "frontend", "pgbus") }

  describe "CSS" do
    it "has a compiled style.css" do
      css = frontend_dir.join("style.css")
      expect(css).to exist
      expect(css.size).to be > 1000 # compiled Tailwind is at least a few KB
    end
  end

  describe "JavaScript vendor files" do
    it "has vendored Turbo" do
      turbo = frontend_dir.join("vendor", "turbo.js")
      expect(turbo).to exist
      content = turbo.read
      expect(content).to include("Turbo")
    end

    it "has vendored ApexCharts" do
      apexcharts = frontend_dir.join("vendor", "apexcharts.js")
      expect(apexcharts).to exist
      content = apexcharts.read(nil, 200)
      expect(content).to include("ApexCharts")
    end
  end

  describe "JavaScript modules" do
    it "has application.js" do
      app_js = frontend_dir.join("application.js")
      expect(app_js).to exist
      content = app_js.read
      expect(content).to include('import * as Turbo from "turbo"')
    end

    it "has charts.js module" do
      charts = frontend_dir.join("modules", "charts.js")
      expect(charts).to exist
      content = charts.read
      expect(content).to include("export function renderCharts")
    end
  end

  describe "route integration" do
    it "includes frontend routes in the routes file" do
      routes_file = Pgbus::Engine.root.join("config", "routes.rb").read
      expect(routes_file).to include("frontend_module")
      expect(routes_file).to include("frontend_static")
    end
  end
end
