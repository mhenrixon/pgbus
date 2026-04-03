# frozen_string_literal: true

require "system_helper"

RSpec.describe "Queues", type: :system do
  describe "index page" do
    it "lists all queues with metrics" do
      visit "/pgbus/queues"

      expect(page).to have_css("h1", text: "Queues")
      expect(page).to have_text("pgbus_default")
      expect(page).to have_text("pgbus_default_dlq")
      expect(page).to have_text("500") # total_messages
    end

    it "shows purge and delete buttons for each queue" do
      visit "/pgbus/queues"

      expect(page).to have_button("Purge", count: 2)
      expect(page).to have_button("Delete", count: 2)
    end

    it "shows pause button for unpaused queues" do
      visit "/pgbus/queues"

      expect(page).to have_button("Pause", count: 2)
      expect(page).not_to have_button("Resume")
    end

    it "shows resume button for paused queues" do
      @stub_data_source.queues = [
        { name: "pgbus_default", queue_length: 10, queue_visible_length: 8,
          oldest_msg_age_sec: 120, newest_msg_age_sec: 5, total_messages: 500, paused: true }
      ]

      visit "/pgbus/queues"

      expect(page).to have_button("Resume", count: 1)
      expect(page).to have_text("Paused")
    end

    it "shows empty state when no queues" do
      @stub_data_source.queues = []

      visit "/pgbus/queues"

      expect(page).to have_text("No queues found")
    end
  end

  describe "show page" do
    it "displays queue name and metrics" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_css("h1", text: "pgbus_default")
      expect(page).to have_text("Depth:")
      expect(page).to have_text("10")
    end

    it "shows purge and delete buttons" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Purge Queue")
      expect(page).to have_button("Delete Queue")
    end

    it "shows pause button when queue is not paused" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Pause")
      expect(page).not_to have_button("Resume")
    end

    it "shows resume button and paused badge when queue is paused" do
      @stub_data_source.paused_queues = ["pgbus_default"]

      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Resume")
      expect(page).not_to have_button("Pause")
      expect(page).to have_text("Paused")
    end

    it "shows empty state when queue has no messages" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_text("Queue is empty")
    end

    it "displays messages when present" do
      @stub_data_source.jobs_list = [
        { msg_id: 42, queue_name: "pgbus_default", read_ct: 3,
          enqueued_at: Time.now.utc.iso8601, vt: Time.now.utc.iso8601,
          message: '{"job_class":"TestJob"}' }
      ]

      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_text("42")
      expect(page).to have_text("3") # read_ct
    end

    it "shows discard and retry buttons for each message" do
      @stub_data_source.jobs_list = [
        { msg_id: 42, queue_name: "pgbus_default", read_ct: 0,
          enqueued_at: Time.now.utc.iso8601, vt: Time.now.utc.iso8601,
          message: '{"job_class":"TestJob"}' }
      ]

      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Discard", count: 1)
      expect(page).to have_button("Retry", count: 1)
    end
  end
end
