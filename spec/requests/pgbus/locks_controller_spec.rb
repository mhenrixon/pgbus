# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::LocksController", type: :request do
  describe "GET /pgbus/locks" do
    it "renders the locks index" do
      get "/pgbus/locks"
      expect(response).to have_http_status(:ok)
    end
  end

  describe "POST /pgbus/locks/:id/discard" do
    it "discards the lock and redirects with a notice" do
      post "/pgbus/locks/some-key/discard"
      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:notice]).to be_present
      expect(@stub_data_source.calls[:discard_lock]).to eq([["some-key"]])
    end
  end

  describe "POST /pgbus/locks/discard_selected" do
    context "when no keys are selected" do
      it "redirects with an alert" do
        post "/pgbus/locks/discard_selected", params: { lock_keys: [""] }
        expect(response).to redirect_to("/pgbus/locks")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:discard_locks)
      end
    end

    context "when keys are selected" do
      it "discards the selected locks" do
        post "/pgbus/locks/discard_selected", params: { lock_keys: %w[a b] }
        expect(response).to redirect_to("/pgbus/locks")
        expect(@stub_data_source.calls[:discard_locks]).to eq([[%w[a b]]])
      end
    end
  end

  describe "POST /pgbus/locks/discard_all" do
    it "discards all locks and redirects" do
      post "/pgbus/locks/discard_all"
      expect(response).to redirect_to("/pgbus/locks")
      expect(@stub_data_source.calls).to have_key(:discard_all_locks)
    end
  end
end
