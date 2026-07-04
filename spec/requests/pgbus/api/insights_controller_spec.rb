# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::Api::InsightsController", type: :request do
  subject(:json) { response.parsed_body }

  describe "GET /pgbus/api/insights" do
    context "when latency columns are unavailable" do
      before do
        allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(false)
        get "/pgbus/api/insights"
      end

      it "returns 200 OK" do
        expect(response).to have_http_status(:ok)
      end

      it "renders the base insights keys" do
        expect(json.keys).to include("summary", "throughput", "status_counts", "slowest", "live_streams")
      end

      it "omits the latency branch keys" do
        expect(json).not_to have_key("latency_trend")
        expect(json).not_to have_key("latency_by_queue")
      end
    end

    context "when latency columns are available" do
      before do
        allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(true)
        get "/pgbus/api/insights"
      end

      it "includes the latency branch keys" do
        expect(json).to have_key("latency_trend")
        expect(json).to have_key("latency_by_queue")
      end
    end
  end
end
