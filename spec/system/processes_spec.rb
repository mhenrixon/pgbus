# frozen_string_literal: true

require "system_helper"

RSpec.describe "Processes", type: :system do
  it "shows processes table" do
    visit "/pgbus/processes"

    expect(page).to have_css("h1", text: "Processes")
    expect(page).to have_text("worker")
    expect(page).to have_text("test-host")
    expect(page).to have_text("12345")
  end

  it "shows healthy status badge" do
    visit "/pgbus/processes"

    expect(page).to have_text("Healthy")
  end

  it "shows empty state" do
    @stub_data_source.processes_list = []

    visit "/pgbus/processes"

    expect(page).to have_text("No processes running")
  end
end
