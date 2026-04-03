# frozen_string_literal: true

require "spec_helper"
require "action_controller"
require "active_support"

RSpec.describe "Pgbus::ApplicationController base class" do # rubocop:disable RSpec/DescribeClass
  describe "with default configuration" do
    it "resolves to ActionController::Base" do
      expect(Pgbus.configuration.base_controller_class).to eq("::ActionController::Base")
      expect("::ActionController::Base".constantize).to eq(ActionController::Base)
    end
  end

  describe "with custom base_controller_class" do
    let(:custom_base) do
      Class.new(ActionController::Base) do
        def self.name
          "TestAdminController"
        end
      end
    end

    before do
      stub_const("TestAdminController", custom_base)
      @original = Pgbus.configuration.base_controller_class
      Pgbus.configuration.base_controller_class = "TestAdminController"
    end

    after do
      Pgbus.configuration.base_controller_class = @original # rubocop:disable RSpec/InstanceVariable
    end

    it "allows constantizing the configured class" do
      klass = Pgbus.configuration.base_controller_class.constantize
      expect(klass).to eq(TestAdminController)
      expect(klass.superclass).to eq(ActionController::Base)
    end
  end
end
