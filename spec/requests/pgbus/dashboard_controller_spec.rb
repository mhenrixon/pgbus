# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::DashboardController", type: :request do
  describe "GET /pgbus" do
    it "renders the dashboard" do
      get "/pgbus"
      expect(response).to have_http_status(:ok)
    end

    %w[stats queues processes failures health].each do |frame|
      it "renders the #{frame} turbo frame without a layout" do
        get "/pgbus", params: { frame: frame }
        expect(response).to have_http_status(:ok)
      end
    end
  end
end
