# frozen_string_literal: true

require "json"

module Pgbus
  module CLI
    # Prints the vendored AppSignal dashboard definitions as import-ready
    # JSON. The files ship in the automated-dashboard format used by
    # appsignal/public_config (a `metric_keys` trigger wrapping a
    # `dashboard` object); AppSignal's "Import dashboard" dialog wants the
    # inner object only, so this command unwraps it:
    #
    #   pgbus dashboard              # main dashboard
    #   pgbus dashboard health      # one of the extra dashboards
    #   pgbus dashboard --list      # available names and titles
    module Dashboard
      module_function

      def start(args)
        name = args.first || "main"
        return list if ["--list", "list"].include?(name)

        path = available[name]
        unless path
          warn "Unknown dashboard: #{name.inspect} (available: #{available.keys.join(", ")})"
          exit 1
        end

        puts JSON.pretty_generate(definition(path))
      end

      def list
        available.each do |name, path|
          puts format("%-12s %s", name, definition(path)["title"])
        end
      end

      # The main dashboard plus the extras, keyed by their short CLI name
      # (pgbus_health.json => "health").
      def available
        require "pgbus/integrations/appsignal"

        extras = Dir[File.join(Integrations::Appsignal::DASHBOARDS_DIR, "*.json")].to_h do |path|
          [File.basename(path, ".json").delete_prefix("pgbus_"), path]
        end
        { "main" => Integrations::Appsignal::DASHBOARD_PATH }.merge(extras)
      end

      def definition(path)
        JSON.parse(File.read(path)).fetch("dashboard")
      end
    end
  end
end
