# frozen_string_literal: true

require "system_helper"

RSpec.describe "Locks", type: :system do
  it "shows empty state" do
    visit "/pgbus/locks"

    expect(page).to have_css("h1", text: "Uniqueness Keys")
    expect(page).to have_text("No active locks")
  end

  context "with locks" do
    before do
      @stub_data_source.locks_list = [
        { lock_key: "import-42", queue_name: "default", msg_id: 123,
          created_at: Time.now.utc, age_seconds: 120 },
        { lock_key: "export-99", queue_name: "urgent", msg_id: 456,
          created_at: Time.now.utc, age_seconds: 60 }
      ]
    end

    it "displays locks with details" do
      visit "/pgbus/locks"

      expect(page).to have_text("import-42")
      expect(page).to have_text("export-99")
      expect(page).to have_text("default")
      expect(page).to have_text("urgent")
    end

    it "shows per-lock discard buttons" do
      visit "/pgbus/locks"

      expect(page).to have_button("Discard", minimum: 2)
    end

    it "shows Discard All button" do
      visit "/pgbus/locks"

      expect(page).to have_button("Discard All")
    end

    it "shows checkboxes for each lock" do
      visit "/pgbus/locks"

      expect(page).to have_css("input[data-bulk-item]", count: 2)
      expect(page).to have_css("input[data-bulk-select-all]")
    end

    it "discard single lock: confirm and shows toast" do
      visit "/pgbus/locks"

      # Click the first per-row Discard button (inside button_to forms)
      within("tbody") do
        all("tr").first.click_button("Discard")
      end
      accept_confirm_dialog

      expect(page).to have_toast("Lock discarded")
      expect(@stub_data_source).to be_called(:discard_lock)
    end

    it "discard all locks: confirm and shows toast" do
      visit "/pgbus/locks"

      click_button "Discard All"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_all_locks)
    end

    it "bulk select and discard selected locks" do
      visit "/pgbus/locks"

      # Initially the "Discard Selected" button is hidden
      expect(page).not_to have_button("Discard Selected", visible: :visible)

      # Check individual checkboxes
      all("input[data-bulk-item]").each(&:check)

      # Now the "Discard Selected" button should appear
      expect(page).to have_button("Discard Selected", visible: :visible)

      click_button "Discard Selected"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_locks)
    end

    it "select all checkbox toggles all items" do
      visit "/pgbus/locks"

      find("input[data-bulk-select-all]").check

      expect(all("input[data-bulk-item]")).to all(be_checked)

      find("input[data-bulk-select-all]").uncheck

      all("input[data-bulk-item]").each do |cb|
        expect(cb).not_to be_checked
      end
    end
  end

  context "without locks" do
    it "does not show action buttons" do
      visit "/pgbus/locks"

      expect(page).not_to have_button("Discard All")
      expect(page).not_to have_button("Discard Selected")
      expect(page).not_to have_css("input[data-bulk-select-all]")
    end
  end
end
