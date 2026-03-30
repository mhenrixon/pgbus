# frozen_string_literal: true

require "system_helper"

RSpec.describe "Queues", type: :system do
  it "lists all queues with metrics" do
    visit "/pgbus/queues"

    expect(page).to have_css("h1", text: "Queues")
    expect(page).to have_text("pgbus_default")
    expect(page).to have_text("pgbus_default_dlq")
    expect(page).to have_text("500") # total_messages
  end

  it "shows purge button for each queue" do
    visit "/pgbus/queues"

    expect(page).to have_button("Purge", count: 2)
  end

  it "shows empty state when no queues" do
    @stub_data_source.queues = []

    visit "/pgbus/queues"

    expect(page).to have_text("No queues found")
  end
end
