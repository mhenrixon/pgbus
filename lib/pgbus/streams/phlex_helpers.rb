# frozen_string_literal: true

module Pgbus
  module Streams
    # Phlex-includable version of the +pgbus_stream_from+ Rails view helper, so
    # a Phlex component can subscribe a page to a pgbus stream the same way it
    # would with turbo-rails' +turbo_stream_from+:
    #
    #   class ChatRoom < Phlex::HTML
    #     include Pgbus::Streams::PhlexHelpers
    #
    #     def view_template
    #       pgbus_stream_from(@room)
    #     end
    #   end
    #
    # phlex-rails doesn't ship a +Phlex::Rails::Helpers::PgbusStreamFrom+, so
    # apps used to hand-register this module locally. It is defined exactly like
    # phlex-rails' own +Phlex::Rails::Helpers::TurboStreamFrom+ (register the
    # method as an output helper resolved against the Rails view context).
    #
    # This file is NOT autoloaded — phlex-rails is an OPTIONAL dependency, so the
    # module references +Phlex::Rails::HelperMacros+, which only exists when the
    # app has phlex-rails. Require it explicitly from a Phlex component or an
    # initializer:  require "pgbus/streams/phlex_helpers".
    module PhlexHelpers
      extend Phlex::Rails::HelperMacros

      register_output_helper def pgbus_stream_from(...) = nil
    end
  end
end
