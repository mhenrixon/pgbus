# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::ProcessesController", type: :request do
  describe "GET /pgbus/processes" do
    it "renders the processes index" do
      get "/pgbus/processes"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/processes", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end
  end
end
