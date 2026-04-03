# frozen_string_literal: true

require "system_helper"

RSpec.describe "Turbo Frames", type: :system do
  it "dashboard has turbo-frame elements with auto-refresh" do
    visit "/pgbus"

    expect(page).to have_css("turbo-frame#dashboard-stats")
    expect(page).to have_css("turbo-frame#dashboard-queues")
    expect(page).to have_css("turbo-frame#dashboard-processes")
    expect(page).to have_css("turbo-frame#dashboard-failures")
  end

  it "queues index has turbo-frame" do
    visit "/pgbus/queues"

    expect(page).to have_css("turbo-frame#queues-list")
  end

  it "jobs index has turbo-frames for failed and enqueued" do
    visit "/pgbus/jobs"

    expect(page).to have_css("turbo-frame#jobs-failed")
    expect(page).to have_css("turbo-frame#jobs-enqueued")
  end

  it "processes index has turbo-frame" do
    visit "/pgbus/processes"

    expect(page).to have_css("turbo-frame#processes-list")
  end

  it "DLQ index has turbo-frame" do
    visit "/pgbus/dlq"

    expect(page).to have_css("turbo-frame#dlq-messages")
  end

  it "dashboard frame endpoint returns only the partial" do
    visit "/pgbus?frame=stats"

    expect(page).to have_css("turbo-frame#dashboard-stats")
    expect(page).not_to have_css("nav")
  end

  it "has custom confirm dialog element" do
    visit "/pgbus"

    expect(page).to have_css("dialog#pgbus-confirm-dialog", visible: :hidden)
    expect(page).to have_css("dialog#pgbus-alert-dialog", visible: :hidden)
  end

  it "has toast container" do
    visit "/pgbus"

    expect(page).to have_css("#pgbus-toast-container")
  end
end
