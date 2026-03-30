# frozen_string_literal: true

require "system_helper"

RSpec.describe "Events", type: :system do
  it "shows empty state for subscribers and events" do
    visit "/pgbus/events"

    expect(page).to have_css("h1", text: "Events")
    expect(page).to have_text("No subscribers registered")
    expect(page).to have_text("No events processed yet")
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
end
