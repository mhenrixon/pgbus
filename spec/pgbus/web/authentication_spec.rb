# frozen_string_literal: true

require "active_support"
require_relative "../../../lib/pgbus/web/authentication"

RSpec.describe Pgbus::Web::Authentication do
  let(:controller_class) do
    Class.new do
      # Simulate before_action
      def self.before_action(method_name)
        @_before_actions ||= []
        @_before_actions << method_name
      end

      def self.before_actions
        @_before_actions || []
      end

      include Pgbus::Web::Authentication

      attr_accessor :request, :head_status

      def head(status)
        @head_status = status
      end
    end
  end

  let(:controller) { controller_class.new }
  let(:request) { double("request") }

  before { controller.request = request }

  describe "#authenticate_pgbus!" do
    context "when web_auth is nil" do
      before { Pgbus.configuration.web_auth = nil }

      it "allows access" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to be_nil
      end
    end

    context "when web_auth returns true" do
      before { Pgbus.configuration.web_auth = ->(_req) { true } }
      after { Pgbus.configuration.web_auth = nil }

      it "allows access" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to be_nil
      end
    end

    context "when web_auth returns false" do
      before { Pgbus.configuration.web_auth = ->(_req) { false } }
      after { Pgbus.configuration.web_auth = nil }

      it "returns unauthorized" do
        controller.send(:authenticate_pgbus!)
        expect(controller.head_status).to eq(:unauthorized)
      end
    end
  end

  it "registers before_action" do
    expect(controller_class.before_actions).to include(:authenticate_pgbus!)
  end
end
