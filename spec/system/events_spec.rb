# frozen_string_literal: true

require "system_helper"

RSpec.describe "Events", type: :system do
  it "shows empty state for subscribers and events" do
    visit "/pgbus/events"

    expect(page).to have_css("h1", text: "Events")
    expect(page).to have_text("No subscribers registered")
    expect(page).to have_text("No events processed yet")
    expect(page).to have_text("No pending events")
  end

  context "with subscribers" do
    before do
      @stub_data_source.subscribers_list = [
        { pattern: "orders.#", handler_class: "OrderHandler", queue_name: "pgbus_orders" }
      ]
    end

    it "displays registered subscribers" do
      visit "/pgbus/events"

      expect(page).to have_text("orders.#")
      expect(page).to have_text("OrderHandler")
      expect(page).to have_text("pgbus_orders")
    end
  end

  context "with processed events" do
    before do
      @stub_data_source.events_list = [
        { "id" => 1, "event_id" => "evt-abc-123", "handler_class" => "OrderHandler",
          "processed_at" => Time.now.utc.iso8601 }
      ]
    end

    it "displays processed events" do
      visit "/pgbus/events"

      expect(page).to have_text("evt-abc-123")
      expect(page).to have_text("OrderHandler")
    end
  end

  context "with pending events" do
    before do
      @stub_data_source.subscribers_list = [
        { pattern: "task.completed", handler_class: "TaskCompletionHandler", queue_name: "task_completion_handler" },
        { pattern: "webhook.*", handler_class: "WebhookHandler", queue_name: "webhook_handler" }
      ]

      @stub_data_source.pending_events_list = [
        {
          msg_id: 42, read_ct: 5,
          enqueued_at: Time.now.utc.iso8601,
          last_read_at: Time.now.utc.iso8601,
          vt: (Time.now + 300).utc.iso8601,
          message: '{"event_id":"evt-stuck-1","payload":{"order_id":123},"published_at":"2026-04-09T00:00:00Z"}',
          headers: nil,
          queue_name: "task_completion_handler"
        }
      ]
    end

    it "displays pending events section" do
      visit "/pgbus/events"

      expect(page).to have_text("Pending Events")
      expect(page).to have_text("42")
      expect(page).to have_text("task_completion_handler")
    end

    it "shows discard button for pending events" do
      visit "/pgbus/events"

      # Expand the details row
      find("details.group summary").click

      expect(page).to have_button("Discard")
    end

    it "shows mark handled button for pending events" do
      visit "/pgbus/events"

      find("details.group summary").click

      expect(page).to have_button("Mark Handled")
    end

    it "shows reroute option for pending events" do
      visit "/pgbus/events"

      find("details.group summary").click

      expect(page).to have_text("Reroute")
    end

    it "shows edit payload section for pending events" do
      visit "/pgbus/events"

      find("details.group summary").click

      expect(page).to have_text("Edit & Retry")
    end

    it "discards a pending event" do
      visit "/pgbus/events"

      find("details.group summary").click
      click_button "Discard"
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source).to be_called(:discard_event)
    end

    it "marks a pending event as handled" do
      visit "/pgbus/events"

      find("details.group summary").click
      click_button "Mark Handled"
      accept_confirm_dialog

      expect(page).to have_toast("handled")
      expect(@stub_data_source).to be_called(:mark_event_handled)
    end
  end
end
