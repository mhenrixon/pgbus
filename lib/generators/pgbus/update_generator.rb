# frozen_string_literal: true

require "rails/generators"
require "pgbus/generators/config_converter"

module Pgbus
  module Generators
    # Converts an existing config/pgbus.yml to a Ruby initializer at
    # config/initializers/pgbus.rb using the modern DSL.
    #
    # The original YAML file is left in place — the user reviews the
    # generated initializer and deletes the YAML when ready.
    #
    # Usage:
    #
    #   bin/rails generate pgbus:update
    #   bin/rails generate pgbus:update --force      # overwrite existing initializer
    #   bin/rails generate pgbus:update --source=path/to/pgbus.yml
    class UpdateGenerator < Rails::Generators::Base
      desc "Convert config/pgbus.yml to config/initializers/pgbus.rb using the Ruby DSL"

      class_option :source,
                   type: :string,
                   default: "config/pgbus.yml",
                   desc: "Path to the existing YAML config (default: config/pgbus.yml)"

      class_option :destination,
                   type: :string,
                   default: "config/initializers/pgbus.rb",
                   desc: "Path to the generated initializer (default: config/initializers/pgbus.rb)"

      def convert
        source_path = File.expand_path(options[:source], destination_root)
        destination_path = File.expand_path(options[:destination], destination_root)

        # Thor::Error is the idiomatic way to abort a Rails generator. Thor
        # catches it, prints the message in red, and exits with status 1
        # without a Ruby backtrace. exit 1 would skip the framework's
        # cleanup hooks and is hard to test.
        raise Thor::Error, "Source file not found: #{options[:source]}" unless File.exist?(source_path)

        ruby_source = load_and_convert(source_path)
        create_file destination_path, ruby_source
      end

      def display_post_install
        say ""
        say "Pgbus initializer generated at #{options[:destination]}!", :green
        say ""
        say "Next steps:"
        say "  1. Review the generated initializer for correctness"
        say "  2. Boot your app and verify everything still works"
        say "  3. Delete #{options[:source]} when satisfied (Pgbus will stop reading it)"
        say ""
        say "If you spot a setting that didn't translate cleanly, please open an issue:"
        say "  https://github.com/mhenrixon/pgbus/issues", :cyan
        say ""
      end

      private

      # Wrap converter and YAML errors as Thor::Error so the generator
      # surfaces them with the standard "in red, no backtrace, exit 1"
      # behavior. Catches:
      #   - ConfigConverter::Error (validation, missing file race)
      #   - Psych::Exception (malformed YAML, disallowed types)
      #   - Errno::ENOENT / Errno::EACCES (file disappeared / not readable)
      def load_and_convert(source_path)
        ConfigConverter.from_yaml(source_path)
      rescue ConfigConverter::Error, Psych::Exception, Errno::ENOENT, Errno::EACCES => e
        raise Thor::Error, "Failed to convert #{options[:source]}: #{e.message}"
      end
    end
  end
end
