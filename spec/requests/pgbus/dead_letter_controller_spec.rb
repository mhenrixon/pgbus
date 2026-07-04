# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::DeadLetterController", type: :request do
  describe "GET /pgbus/dlq" do
    it "renders the dead letter index" do
      get "/pgbus/dlq"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/dlq", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end
  end

  describe "GET /pgbus/dlq/:id" do
    context "when the message exists" do
      before { @stub_data_source.dlq_messages_list = [{ msg_id: 12, message: {} }] }

      it "renders the message detail" do
        get "/pgbus/dlq/12"
        expect(response).to have_http_status(:ok)
      end
    end
  end

  describe "POST /pgbus/dlq/:id/retry" do
    context "with a valid _dlq queue" do
      it "re-enqueues the message" do
        post "/pgbus/dlq/12/retry", params: { queue_name: "pgbus_default_dlq" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:retry_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end

    context "with a non-DLQ queue" do
      it "rejects with an alert and does not touch the data source" do
        post "/pgbus/dlq/12/retry", params: { queue_name: "pgbus_default" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to eq("Invalid DLQ queue.")
        expect(@stub_data_source.calls).not_to have_key(:retry_dlq_message)
      end
    end
  end

  describe "POST /pgbus/dlq/:id/discard" do
    context "with a valid _dlq queue" do
      it "discards the message" do
        post "/pgbus/dlq/12/discard", params: { queue_name: "pgbus_default_dlq" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:discard_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end

    context "with a non-DLQ queue" do
      it "rejects with an alert" do
        post "/pgbus/dlq/12/discard", params: { queue_name: "pgbus_default" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to eq("Invalid DLQ queue.")
        expect(@stub_data_source.calls).not_to have_key(:discard_dlq_message)
      end
    end
  end

  describe "POST /pgbus/dlq/retry_all" do
    it "re-enqueues all DLQ messages" do
      post "/pgbus/dlq/retry_all"
      expect(response).to redirect_to("/pgbus/dlq")
      expect(@stub_data_source.calls).to have_key(:retry_all_dlq)
    end
  end

  describe "POST /pgbus/dlq/discard_all" do
    it "discards all DLQ messages" do
      post "/pgbus/dlq/discard_all"
      expect(response).to redirect_to("/pgbus/dlq")
      expect(@stub_data_source.calls).to have_key(:discard_all_dlq)
    end
  end

  describe "POST /pgbus/dlq/discard_selected" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/dlq/discard_selected", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to be_present
      end
    end

    context "when a valid _dlq selection is present" do
      it "discards the selected DLQ message" do
        post "/pgbus/dlq/discard_selected",
             params: { messages: [{ queue_name: "pgbus_default_dlq", msg_id: "12" }] }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:discard_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end
  end
end
