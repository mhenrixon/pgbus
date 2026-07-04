# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::EventsController", type: :request do
  describe "GET /pgbus/events" do
    it "renders the events index" do
      get "/pgbus/events"
      expect(response).to have_http_status(:ok)
    end
  end

  describe "POST /pgbus/events/:id/replay" do
    context "when the processed event exists" do
      before { @stub_data_source.events_list = [{ "id" => "5", "event_type" => "orders.created" }] }

      it "replays the event" do
        post "/pgbus/events/5/replay"
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:notice]).to be_present
        expect(@stub_data_source.calls).to have_key(:replay_event)
      end
    end

    context "when the processed event is unknown" do
      it "redirects with a failure alert" do
        post "/pgbus/events/999/replay"
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:replay_event)
      end
    end
  end

  describe "POST /pgbus/events/:id/discard" do
    context "when the queue is not a registered handler queue" do
      it "rejects with an alert and does not touch the data source" do
        post "/pgbus/events/5/discard", params: { queue_name: "pgbus_arbitrary" }
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:discard_event)
      end
    end

    context "when the queue is a registered handler queue" do
      before do
        @stub_data_source.subscribers_list = [
          { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
        ]
      end

      it "discards the event" do
        post "/pgbus/events/5/discard", params: { queue_name: "pgbus_orders" }
        expect(response).to redirect_to("/pgbus/events")
        expect(@stub_data_source.calls[:discard_event]).to eq([%w[pgbus_orders 5]])
      end
    end
  end

  describe "POST /pgbus/events/:id/mark_handled" do
    before do
      @stub_data_source.subscribers_list = [
        { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
      ]
    end

    it "resolves the handler class server-side and marks the event handled" do
      post "/pgbus/events/5/mark_handled", params: { queue_name: "pgbus_orders", handler_class: "Evil" }
      expect(response).to redirect_to("/pgbus/events")
      expect(@stub_data_source.calls[:mark_event_handled]).to eq([%w[pgbus_orders 5 OrdersHandler]])
    end
  end

  describe "POST /pgbus/events/:id/reroute" do
    before do
      @stub_data_source.subscribers_list = [
        { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" },
        { physical_queue_name: "pgbus_audit", handler_class: "AuditHandler" }
      ]
    end

    it "reroutes between two registered queues" do
      post "/pgbus/events/5/reroute", params: { queue_name: "pgbus_orders", target_queue: "pgbus_audit" }
      expect(response).to redirect_to("/pgbus/events")
      expect(@stub_data_source.calls[:reroute_event]).to eq([%w[pgbus_orders 5 pgbus_audit]])
    end

    it "rejects rerouting to an unregistered target queue" do
      post "/pgbus/events/5/reroute", params: { queue_name: "pgbus_orders", target_queue: "pgbus_evil" }
      expect(response).to redirect_to("/pgbus/events")
      expect(flash[:alert]).to be_present
      expect(@stub_data_source.calls).not_to have_key(:reroute_event)
    end
  end

  describe "POST /pgbus/events/discard_selected" do
    context "when no valid selections" do
      it "redirects with an alert" do
        post "/pgbus/events/discard_selected", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
      end
    end

    context "when selections reference registered queues" do
      before do
        @stub_data_source.subscribers_list = [
          { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
        ]
      end

      it "discards the selected events" do
        post "/pgbus/events/discard_selected",
             params: { messages: [{ queue_name: "pgbus_orders", msg_id: "9" }] }
        expect(response).to redirect_to("/pgbus/events")
        expect(@stub_data_source.calls[:discard_selected_events]).to eq([[[{ queue_name: "pgbus_orders", msg_id: "9" }]]])
      end
    end
  end
end
