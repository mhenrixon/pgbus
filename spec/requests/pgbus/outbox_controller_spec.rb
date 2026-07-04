# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::OutboxController", type: :request do
  describe "GET /pgbus/outbox" do
    it "renders the outbox index" do
      get "/pgbus/outbox"
      expect(response).to have_http_status(:ok)
    end
  end
end
