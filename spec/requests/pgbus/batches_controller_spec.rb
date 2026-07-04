# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::BatchesController", type: :request do
  describe "GET /pgbus/batches" do
    it "renders the batches index" do
      get "/pgbus/batches"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/batches", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end
  end

  describe "GET /pgbus/batches/:id" do
    context "when the batch is unknown" do
      it "redirects to the index with a not-found alert" do
        get "/pgbus/batches/does-not-exist"
        expect(response).to redirect_to("/pgbus/batches")
        expect(flash[:alert]).to be_present
      end
    end
  end
end
