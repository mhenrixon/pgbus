# frozen_string_literal: true

require "spec_helper"
require "action_controller"
require_relative "../../../app/controllers/pgbus/frontends_controller"

RSpec.describe Pgbus::FrontendsController do
  describe "STATIC_ASSETS" do
    it "references only files that exist" do
      described_class::STATIC_ASSETS.each_value do |entries|
        entries.each do |name, path|
          expect(path).to exist, "Static asset #{name} not found at #{path}"
        end
      end
    end
  end

  describe "MODULE_OVERRIDES" do
    it "references only files that exist" do
      described_class::MODULE_OVERRIDES.each do |name, path|
        expect(path).to exist, "Module override #{name} not found at #{path}"
      end
    end
  end

  describe ".js_modules" do
    it "includes application and turbo modules" do
      modules = described_class.js_modules
      expect(modules).to include(:application, :turbo, :charts)
    end

    it "references only files that exist" do
      described_class.js_modules.each do |name, path|
        expect(path).to exist, "JS module #{name} not found at #{path}"
      end
    end
  end

  describe "inherits from ActionController::Base" do
    it "does not inherit from Pgbus::ApplicationController" do
      expect(described_class.superclass).to eq(ActionController::Base)
    end
  end
end
