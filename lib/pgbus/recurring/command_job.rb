# frozen_string_literal: true

module Pgbus
  module Recurring
    # Job class for command-based recurring tasks.
    # Executes a Ruby command string, similar to solid_queue's RecurringJob.
    #
    # NOTE: Only use this with trusted commands from config/recurring.yml.
    # Never expose this to user input.
    class CommandJob < ::ActiveJob::Base
      queue_as :pgbus_recurring

      def perform(command)
        eval(command) # rubocop:disable Security/Eval
      end
    end
  end
end
