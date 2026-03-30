# frozen_string_literal: true

require "active_job"

# ActiveJob 8.1 does not have QueueAdapters.register.
# The adapter source file calls it at load time, so we provide a no-op
# to avoid NoMethodError in the test suite.
unless ActiveJob::QueueAdapters.respond_to?(:register)
  module ActiveJob
    module QueueAdapters
      def self.register(_name, _klass) = nil
    end
  end
end
