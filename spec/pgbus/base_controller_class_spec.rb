# frozen_string_literal: true

require "spec_helper"

RSpec.describe "base_controller_class configuration" do # rubocop:disable RSpec/DescribeClass
  describe "default" do
    it "defaults to ::ActionController::Base" do
      config = Pgbus::Configuration.new
      expect(config.base_controller_class).to eq("::ActionController::Base")
    end
  end

  describe "custom value" do
    it "accepts a string class name" do
      config = Pgbus::Configuration.new
      config.base_controller_class = "Admin::BaseController"
      expect(config.base_controller_class).to eq("Admin::BaseController")
    end
  end

  describe "Pgbus.base_controller_class" do
    it "delegates to configuration" do
      expect(Pgbus.configuration.base_controller_class).to eq("::ActionController::Base")
    end
  end
end
