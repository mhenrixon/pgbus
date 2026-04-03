# frozen_string_literal: true

module DialogHelpers
  # Accept the custom <dialog>-based confirm by clicking the Confirm/Delete button.
  def accept_confirm_dialog
    dialog = find("dialog#pgbus-confirm-dialog", visible: true)
    dialog.find("button[value='confirm']").click
  end

  # Dismiss the custom <dialog>-based confirm by clicking Cancel.
  def dismiss_confirm_dialog
    dialog = find("dialog#pgbus-confirm-dialog", visible: true)
    dialog.find("button[value='cancel']").click
  end

  # Wait for a toast notification with the given text.
  def have_toast(text)
    have_css("#pgbus-toast-container", text: text)
  end
end

RSpec.configure do |config|
  config.include DialogHelpers, type: :system
end
