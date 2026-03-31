# frozen_string_literal: true

module Pgbus
  module Recurring
    # Job class for command-based recurring tasks.
    # Evaluates a method call chain on a constant, e.g. "OldRecord.cleanup!".
    #
    # Only supports `ConstantName.method_name` and `ConstantName.method_name(args)`.
    # Rejects arbitrary Ruby expressions for safety.
    class CommandJob < ::ActiveJob::Base
      SAFE_COMMAND = /\A([A-Z][A-Za-z0-9_]*(?:::[A-Z][A-Za-z0-9_]*)*)\.([a-z_][a-z0-9_!?]*)\z/

      def perform(command)
        match = SAFE_COMMAND.match(command)
        unless match
          raise ArgumentError,
                "Unsafe recurring command: #{command.inspect}. " \
                "Must be in the form 'ClassName.method_name'."
        end

        klass = match[1].safe_constantize
        raise ArgumentError, "Unknown class in recurring command: #{match[1]}" unless klass

        klass.public_send(match[2])
      end
    end
  end
end
