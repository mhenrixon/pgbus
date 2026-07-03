# frozen_string_literal: true

require "uri"
require "pgbus/mcp/health_analyzer"

module Pgbus
  # Preflight diagnostics for a pgbus deployment — the single command that
  # answers "is this environment healthy enough to run?". Runs six checks and
  # returns a machine-readable result plus a human report, so `pgbus doctor`
  # and `rake pgbus:doctor` can gate a deploy or CI run (exit 0 on success,
  # 1 on any failure).
  #
  # Doctor never touches PGMQ or PostgreSQL directly: every probe goes through
  # Pgbus::Client (DB, PGMQ schema, queues, NOTIFY) or Pgbus::Web::DataSource
  # (process liveness, via Pgbus::MCP::HealthAnalyzer). It also never raises —
  # a broken environment turns into :fail results, never a crash — so it is
  # safe to run against a database that is down or a half-installed schema.
  class Doctor
    # A single check result. status is one of :ok, :warn, :fail.
    Check = Struct.new(:name, :status, :detail) do
      def to_h
        { name: name, status: status, detail: detail }
      end
    end

    STATUS_ICON = { ok: "✓", warn: "!", fail: "✗" }.freeze

    def initialize(config: Pgbus.configuration, client: Pgbus.client, data_source: nil)
      @config = config
      @client = client
      @data_source = data_source || Pgbus::Web::DataSource.new(client: client)
    end

    # Run all checks and return an array of result hashes:
    #   { name:, status: :ok|:warn|:fail, detail: }
    def run
      @run ||= [
        check_configuration,
        check_database,
        check_pgmq_schema,
        check_queues,
        check_notify,
        check_processes
      ].map(&:to_h)
    end

    # True when no check failed. Warnings do not fail the run — they surface a
    # concern (e.g. an outdated PGMQ schema) without blocking a deploy.
    def success?
      run.none? { |c| c[:status] == :fail }
    end

    # Human-readable report: one line per check, then a resolved-config summary
    # with passwords redacted. Suitable for stdout in the CLI and rake task.
    def report
      lines = ["Pgbus Doctor", "=" * 40]
      run.each do |check|
        icon = STATUS_ICON.fetch(check[:status], "?")
        lines << format("%<icon>s %<name>-22s %<detail>s", icon: icon, name: check[:name], detail: check[:detail])
      end
      lines << ""
      lines << "Configuration"
      lines << ("-" * 40)
      config_summary.each { |key, value| lines << format("  %<key>-20s %<value>s", key: key, value: value) }
      lines.join("\n")
    end

    # Resolved-config summary for the report. Password-bearing fields
    # (database_url, connection_params) are redacted.
    def config_summary
      {
        queue_prefix: @config.queue_prefix,
        default_queue: @config.default_queue,
        pgmq_schema_mode: @config.pgmq_schema_mode,
        resolved_pool_size: safe { @config.resolved_pool_size },
        listen_notify: @config.listen_notify,
        roles: @config.roles || "all",
        capsules: capsule_summary,
        database_url: redacted_database_url,
        connection_params: redacted_connection_params
      }.compact
    end

    private

    # 1. Configuration validity.
    def check_configuration
      @config.validate!
      Check.new(name: "Configuration", status: :ok, detail: "valid")
    rescue ArgumentError => e
      Check.new(name: "Configuration", status: :fail, detail: e.message)
    rescue StandardError => e
      Check.new(name: "Configuration", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 2. Database connectivity — SELECT 1 via Client#ping.
    def check_database
      @client.ping
      Check.new(name: "Database", status: :ok, detail: "reachable")
    rescue StandardError => e
      Check.new(name: "Database", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 3. PGMQ schema presence + installed-vs-vendored version.
    #
    # A nil version does NOT automatically mean "not installed": PGMQ installed
    # via the extension, or before pgbus added version tracking, leaves the
    # pgmq schema fully working with no row in pgbus_pgmq_schema_versions. So we
    # distinguish, mirroring `pgbus:pgmq:status`: schema absent → fail; schema
    # present but untracked → warn; tracked but behind → warn; current → ok.
    def check_pgmq_schema
      installed = @client.pgmq_schema_version
      latest = Pgbus::PgmqSchema.latest_version

      if installed.nil?
        unless @client.pgmq_installed?
          return Check.new(name: "PGMQ schema", status: :fail,
                           detail: "not installed — run `rails generate pgbus:install`")
        end

        return Check.new(name: "PGMQ schema", status: :warn,
                         detail: "installed but no version tracking — run `rails generate pgbus:upgrade_pgmq`")
      end

      if Gem::Version.new(installed) < Gem::Version.new(latest)
        Check.new(name: "PGMQ schema", status: :warn,
                  detail: "installed #{installed}, vendored #{latest} — run `rails generate pgbus:upgrade_pgmq`")
      else
        Check.new(name: "PGMQ schema", status: :ok, detail: "up to date (#{installed})")
      end
    rescue StandardError => e
      Check.new(name: "PGMQ schema", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 4. Queue existence — every configured queue must have its PGMQ table(s).
    # Resolve each logical queue to the PHYSICAL names bootstrap creates (via the
    # client's queue strategy) so a priority queue's _p0.._pN sub-tables are what
    # we diff against list_queues — not the bare prefixed name priority mode
    # never creates.
    def check_queues
      configured = @client.configured_queues.flat_map { |q| @client.physical_queue_names(q) }
      existing = existing_queue_names
      missing = configured - existing

      if missing.empty?
        Check.new(name: "Queues", status: :ok, detail: "#{configured.size} configured queue(s) present")
      else
        Check.new(name: "Queues", status: :fail, detail: "missing queue(s): #{missing.join(", ")}")
      end
    rescue StandardError => e
      Check.new(name: "Queues", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 5. LISTEN/NOTIFY — when configured on, every configured queue should carry
    # the insert-NOTIFY trigger. A missing trigger is a warning, not a failure:
    # pgbus still works (it falls back to polling), it just loses instant wakeup.
    def check_notify
      return Check.new(name: "LISTEN/NOTIFY", status: :ok, detail: "disabled in config") unless @config.listen_notify

      without_trigger = @client.configured_queues.reject { |q| @client.notify_enabled?(q) }
      if without_trigger.empty?
        Check.new(name: "LISTEN/NOTIFY", status: :ok, detail: "triggers live on all configured queues")
      else
        Check.new(name: "LISTEN/NOTIFY", status: :warn,
                  detail: "no insert trigger on: #{without_trigger.join(", ")} (falling back to polling)")
      end
    rescue StandardError => e
      Check.new(name: "LISTEN/NOTIFY", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 6. Process liveness — the top-level health verdict (OK/DEGRADED/STALLED)
    # from the shared HealthAnalyzer. STALLED (the silent-worker-wedge) is a
    # failure; DEGRADED is a warning; OK passes.
    def check_processes
      verdict = Pgbus::MCP::HealthAnalyzer.new(@data_source).verdict
      status = verdict[:status]
      detail = Array(verdict[:reasons]).first || status

      case status
      when "STALLED" then Check.new(name: "Process liveness", status: :fail, detail: detail)
      when "DEGRADED" then Check.new(name: "Process liveness", status: :warn, detail: detail)
      else Check.new(name: "Process liveness", status: :ok, detail: "OK")
      end
    rescue StandardError => e
      Check.new(name: "Process liveness", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # Physical queue names known to PGMQ. list_queues rows may be Hashes (with a
    # :queue_name key) or value objects responding to #queue_name.
    def existing_queue_names
      Array(@client.list_queues).map do |row|
        if row.respond_to?(:queue_name)
          row.queue_name
        elsif row.is_a?(Hash)
          row[:queue_name] || row["queue_name"]
        end
      end.compact
    end

    def capsule_summary
      Array(@config.workers).map { |w| w[:name] || (w[:queues] || []).join("+") }.join(", ")
    end

    # Redact the password from a libpq URL/URI while keeping host/db visible so
    # the operator can still confirm which database they pointed at.
    def redacted_database_url
      return nil unless @config.database_url

      redact_url(@config.database_url)
    end

    # connection_params is a free-form libpq keyword hash, so redact every key
    # whose name looks secret (password, sslpassword, ...) — not just :password —
    # rather than assuming a fixed shape. Preserves symbol/string key form.
    SECRET_KEY_PATTERN = /pass|secret|token/i
    private_constant :SECRET_KEY_PATTERN

    def redacted_connection_params
      params = @config.connection_params
      return nil unless params.is_a?(Hash)

      params.each_with_object({}) do |(key, value), out|
        out[key] = key.to_s.match?(SECRET_KEY_PATTERN) ? "[REDACTED]" : value
      end
    end

    # Replace the password in a userinfo authority (scheme://user:pass@host) or a
    # key=value conninfo string (password=secret). Falls back to a blanket
    # redaction label if parsing fails, never leaking the original.
    #
    # The userinfo password can itself contain '@' and ':' (common in generated
    # secrets), so match it greedily up to the LAST '@' before the authority's
    # host — a lazy `[^@]+` would stop at the first '@' and leak the remainder.
    def redact_url(url)
      redacted = url.sub(%r{(://[^:/@]+:).+(@[^@/]*(?:[/?]|\z))}, '\1[REDACTED]\2')
      redacted.gsub(/(\bpassword=)('[^']*'|[^\s'"]+)/i, '\1[REDACTED]')
    rescue StandardError
      "[REDACTED]"
    end

    # Wrap a value-producing block so a broken resolver (e.g. resolved_pool_size
    # raising on a malformed worker config) degrades to nil instead of raising
    # out of config_summary.
    def safe
      yield
    rescue StandardError
      nil
    end
  end
end
