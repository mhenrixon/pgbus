# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::FrontendsController", type: :request do
  let(:version) { Pgbus::VERSION.tr(".", "-") }

  describe "GET /pgbus/frontend/static/:version/:id" do
    it "serves a known static asset for the current version" do
      get "/pgbus/frontend/static/#{version}/style.css"
      expect(response).to have_http_status(:ok)
    end

    it "returns 404 for an unknown asset id" do
      get "/pgbus/frontend/static/#{version}/nope"
      expect(response).to have_http_status(:not_found)
    end

    it "returns 404 for a stale version" do
      get "/pgbus/frontend/static/0-0-0/style.css"
      expect(response).to have_http_status(:not_found)
    end
  end

  describe "GET /pgbus/frontend/modules/:version/:id" do
    it "serves a known JS module for the current version" do
      get "/pgbus/frontend/modules/#{version}/application.js"
      expect(response).to have_http_status(:ok)
    end

    it "returns 404 for an unknown module id" do
      get "/pgbus/frontend/modules/#{version}/nope.js"
      expect(response).to have_http_status(:not_found)
    end
  end
end
