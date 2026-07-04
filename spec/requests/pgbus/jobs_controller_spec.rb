# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::JobsController", type: :request do
  describe "GET /pgbus/jobs" do
    it "renders the jobs index" do
      get "/pgbus/jobs"
      expect(response).to have_http_status(:ok)
    end

    it "renders the failed turbo frame" do
      get "/pgbus/jobs", params: { frame: "failed" }
      expect(response).to have_http_status(:ok)
    end

    it "renders the enqueued turbo frame" do
      get "/pgbus/jobs", params: { frame: "enqueued" }
      expect(response).to have_http_status(:ok)
    end
  end

  describe "POST /pgbus/jobs/:id/retry" do
    it "re-enqueues the failed job" do
      post "/pgbus/jobs/7/retry"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(flash[:notice]).to eq("Job re-enqueued.")
      expect(@stub_data_source.calls[:retry_failed_event]).to eq([["7"]])
    end
  end

  describe "POST /pgbus/jobs/:id/discard" do
    it "discards the failed job" do
      post "/pgbus/jobs/7/discard"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(flash[:notice]).to eq("Job discarded.")
      expect(@stub_data_source.calls[:discard_failed_event]).to eq([["7"]])
    end
  end

  describe "POST /pgbus/jobs/retry_all" do
    it "re-enqueues all failed jobs" do
      post "/pgbus/jobs/retry_all"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:retry_all_failed)
    end
  end

  describe "POST /pgbus/jobs/discard_all" do
    it "discards all failed jobs" do
      post "/pgbus/jobs/discard_all"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:discard_all_failed)
    end
  end

  describe "POST /pgbus/jobs/discard_all_enqueued" do
    it "discards all enqueued jobs" do
      post "/pgbus/jobs/discard_all_enqueued"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:discard_all_enqueued)
    end
  end

  describe "POST /pgbus/jobs/discard_selected_failed" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/jobs/discard_selected_failed", params: { ids: ["0"] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(flash[:alert]).to be_present
      end
    end

    context "when ids are selected" do
      it "discards each selected failed event" do
        post "/pgbus/jobs/discard_selected_failed", params: { ids: %w[3 4] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(@stub_data_source.calls[:discard_failed_event]).to eq([[3], [4]])
      end
    end
  end

  describe "POST /pgbus/jobs/discard_selected_enqueued" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/jobs/discard_selected_enqueued", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(flash[:alert]).to be_present
      end
    end

    context "when selections are present" do
      it "discards each selected enqueued message" do
        post "/pgbus/jobs/discard_selected_enqueued",
             params: { messages: [{ queue_name: "pgbus_default", msg_id: "9" }] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(@stub_data_source.calls[:discard_job]).to eq([%w[pgbus_default 9]])
      end
    end
  end
end
