# frozen_string_literal: true

module Pgbus
  # Shared internal helpers.
  module Support
    module_function

    # Call a user-supplied key proc with a job's arguments, preserving
    # ActiveJob's keyword-argument convention: a trailing Hash whose keys are
    # all Symbols is splatted as keywords; everything else is positional.
    # Single implementation for Uniqueness.resolve_key and
    # Concurrency.resolve_key so the dispatch semantics cannot drift apart.
    def call_key_proc(key_proc, args)
      last = args.last
      if last.is_a?(Hash) && last.each_key.all?(Symbol)
        key_proc.call(*args[...-1], **last)
      else
        key_proc.call(*args)
      end
    end
  end
end
