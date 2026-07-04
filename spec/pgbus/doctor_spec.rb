# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Doctor do
  subject(:doctor) { described_class.new(config: config, client: client, data_source: data_source) }

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://user:s3cret@localhost:5432/pgbus_test"
      c.queue_prefix = "pgbus"
      c.default_queue = "default"
      c.listen_notify = true
      c.pgmq_schema_mode = :auto
    end
  end

  let(:client) do
    build_mock_client.tap do |c|
      allow(c).to receive_messages(
        ping: true,
        pgmq_installed?: true,
        pgmq_schema_version: Pgbus::PgmqSchema.latest_version,
        configured_queues: %w[default],
        list_queues: [{ queue_name: "pgbus_default" }],
        notify_enabled?: true
      )
      # Mirror the real Client: resolve a logical name to its physical PGMQ
      # table name(s). Default (non-priority) maps 1:1 with the prefix.
      allow(c).to receive(:physical_queue_names) { |name| ["pgbus_#{name}"] }
    end
  end

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:ok_verdict) { { status: "OK", reasons: [], summary: { queues: 1, workers: 1 } } }

  before do
    allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {})
    health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: ok_verdict)
    allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)
  end

  describe "#run" do
    it "runs seven checks" do
      expect(doctor.run.size).to eq(7)
    end

    it "returns hashes with :name, :status, :detail keys" do
      doctor.run.each do |check|
        expect(check).to include(:name, :status, :detail)
        expect(check[:status]).to be_in(%i[ok warn fail])
      end
    end

    it "reports all checks as :ok in a healthy environment" do
      expect(doctor.run.map { |c| c[:status] }).to all(eq(:ok))
    end

    it "includes a configuration check" do
      names = doctor.run.map { |c| c[:name] }
      expect(names).to include(a_string_matching(/config/i))
    end

    it "includes checks for db, pgmq, queues, notify, and processes" do
      names = doctor.run.map { |c| c[:name].downcase }.join(" ")
      expect(names).to match(/database|connect/)
      expect(names).to match(/pgmq|schema/)
      expect(names).to include("queue")
      expect(names).to include("notify")
      expect(names).to match(/process|liveness|health/)
    end
  end

  describe "#success?" do
    it "is true when every check passes" do
      expect(doctor.success?).to be(true)
    end

    it "is true when only warnings are present (no failures)" do
      allow(client).to receive(:notify_enabled?).and_return(false)
      config.listen_notify = false

      expect(doctor.success?).to be(true)
    end

    it "is false when any check fails" do
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect")) if defined?(PG::Error)
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect"))

      expect(doctor.success?).to be(false)
    end
  end

  describe "the configuration check" do
    it "fails with the validate! message when the config is invalid" do
      config.pool_timeout = -1

      check = config_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("pool_timeout")
    end

    it "renders the bare validate! message (no ClassName: prefix) for a ConfigurationError" do
      # validate! raises Pgbus::ConfigurationError (#282); the check must catch
      # it in the clean-message rescue, not fall through to the StandardError
      # fallback that prefixes the error class.
      config.pool_timeout = -1

      detail = config_check(doctor.run)[:detail]
      expect(detail).not_to include("Pgbus::ConfigurationError")
      expect(detail).to start_with("pool_timeout")
    end
  end

  describe "the database connectivity check" do
    it "fails when ping raises, and success? is false" do
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("could not connect to server"))

      check = db_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("could not connect to server")
      expect(doctor.success?).to be(false)
    end
  end

  describe "the PGMQ schema check" do
    it "warns when the installed version is older than the vendored version" do
      allow(client).to receive(:pgmq_schema_version).and_return("0.0.1")

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to match(/upgrade|update/i)
    end

    it "fails when the PGMQ schema is genuinely absent (no meta table)" do
      allow(client).to receive_messages(pgmq_schema_version: nil, pgmq_installed?: false)

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to match(/not installed/i)
    end

    it "warns (not fails) when PGMQ is installed but has no version tracking row" do
      # Extension install / an install predating version tracking: pgmq.meta
      # exists and works, but pgbus_pgmq_schema_versions was never populated.
      allow(client).to receive_messages(pgmq_schema_version: nil, pgmq_installed?: true)

      check = pgmq_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to match(/tracking|upgrade_pgmq/i)
    end
  end

  describe "the queue existence check" do
    it "fails and names the missing queue" do
      allow(client).to receive_messages(configured_queues: %w[default critical], list_queues: [{ queue_name: "pgbus_default" }])

      check = queue_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("pgbus_critical")
    end

    it "accepts list_queues rows that respond to queue_name" do
      row = double("QueueRow", queue_name: "pgbus_default")
      allow(client).to receive_messages(configured_queues: %w[default], list_queues: [row])

      expect(queue_check(doctor.run)[:status]).to eq(:ok)
    end

    it "resolves priority sub-queues so a priority deployment is not falsely failed" do
      # Priority mode creates _p0.._pN physical tables, never the bare name.
      allow(client).to receive(:physical_queue_names)
        .with("default").and_return(%w[pgbus_default_p0 pgbus_default_p1 pgbus_default_p2])
      allow(client).to receive_messages(
        configured_queues: %w[default],
        list_queues: [{ queue_name: "pgbus_default_p0" }, { queue_name: "pgbus_default_p1" },
                      { queue_name: "pgbus_default_p2" }]
      )

      expect(queue_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "the LISTEN/NOTIFY check" do
    it "is ok when notify is enabled on all configured queues" do
      expect(notify_check(doctor.run)[:status]).to eq(:ok)
    end

    it "warns (not fails) when listen_notify is configured but a trigger is missing" do
      allow(client).to receive(:notify_enabled?).and_return(false)

      check = notify_check(doctor.run)
      expect(check[:status]).to eq(:warn)
    end

    it "is ok when listen_notify is disabled in config" do
      config.listen_notify = false
      allow(client).to receive(:notify_enabled?).and_return(false)

      expect(notify_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "the process liveness check" do
    it "fails on a STALLED verdict" do
      stalled = { status: "STALLED", reasons: ["worker wedged"], summary: {} }
      health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: stalled)
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)

      check = process_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("worker wedged")
      expect(doctor.success?).to be(false)
    end

    it "warns on a DEGRADED verdict" do
      degraded = { status: "DEGRADED", reasons: ["1 process(es) stale"], summary: {} }
      health = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: degraded)
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_return(health)

      expect(process_check(doctor.run)[:status]).to eq(:warn)
    end

    it "never raises when the HealthAnalyzer blows up" do
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_raise(StandardError, "boom")

      check = process_check(doctor.run)
      expect(check[:status]).to eq(:fail)
      expect(check[:detail]).to include("boom")
    end
  end

  describe "the allowed_global_id_models check (security)" do
    it "is :ok outside production even when allowed_global_id_models is nil" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(false)

      expect(gid_check(doctor.run)[:status]).to eq(:ok)
    end

    it "warns in production when allowed_global_id_models is nil (allow-all)" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(true)

      check = gid_check(doctor.run)
      expect(check[:status]).to eq(:warn)
      expect(check[:detail]).to include("allowed_global_id_models")
    end

    it "does not fail the run — a warning never blocks a deploy" do
      config.allowed_global_id_models = nil
      allow(doctor).to receive(:production?).and_return(true)

      expect(doctor.success?).to be(true)
    end

    it "is :ok in production when an allowlist is configured" do
      config.allowed_global_id_models = %w[User]
      allow(doctor).to receive(:production?).and_return(true)

      expect(gid_check(doctor.run)[:status]).to eq(:ok)
    end
  end

  describe "#report" do
    it "renders one line per check" do
      report = doctor.report
      doctor.run.each { |c| expect(report).to include(c[:name]) }
    end

    it "includes a configuration summary" do
      report = doctor.report
      expect(report).to include("queue_prefix")
      expect(report).to include("pgmq_schema_mode")
    end

    it "redacts the password in the database_url" do
      expect(doctor.report).not_to include("s3cret")
    end
  end

  describe "#config_summary" do
    it "redacts the password from a database_url string" do
      expect(doctor.config_summary[:database_url]).not_to include("s3cret")
      expect(doctor.config_summary[:database_url]).to include("localhost")
    end

    it "redacts a password that itself contains an @ (regex must not stop at the first @)" do
      config.database_url = "postgres://user:p@ss:w0rd@localhost:5432/pgbus_test"

      redacted = doctor.config_summary[:database_url]
      expect(redacted).not_to include("p@ss:w0rd")
      expect(redacted).to include("localhost")
    end

    it "redacts a password= parameter in a conninfo-style database_url" do
      config.database_url = "host=localhost dbname=pgbus password=topsecret sslmode=require"

      expect(doctor.config_summary[:database_url]).not_to include("topsecret")
    end

    it "redacts the password from connection_params hash form" do
      config.database_url = nil
      config.connection_params = { host: "localhost", dbname: "pgbus", password: "hunter2" }

      summary = doctor.config_summary
      expect(summary[:connection_params].to_s).not_to include("hunter2")
    end

    it "redacts other secret-bearing connection_params keys (sslpassword)" do
      config.database_url = nil
      config.connection_params = { host: "localhost", sslpassword: "keysecret", passfile: "/etc/pgpass" }

      summary = doctor.config_summary
      expect(summary[:connection_params].to_s).not_to include("keysecret")
    end

    it "exposes resolved runtime knobs" do
      summary = doctor.config_summary
      expect(summary).to include(:queue_prefix, :default_queue, :pgmq_schema_mode, :resolved_pool_size)
    end
  end

  describe "resilience" do
    it "never raises from #run on a fully broken environment" do
      stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
      allow(client).to receive(:ping).and_raise(PG::Error.new("dead"))
      allow(client).to receive(:pgmq_schema_version).and_raise(StandardError, "nope")
      allow(client).to receive(:list_queues).and_raise(StandardError, "nope")
      allow(client).to receive(:notify_enabled?).and_raise(StandardError, "nope")
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_raise(StandardError, "nope")

      expect { doctor.run }.not_to raise_error
      expect(doctor.success?).to be(false)
    end
  end

  # --- helpers to pluck a check out of the results by keyword ---

  def config_check(checks)  = checks.find { |c| c[:name].match?(/config/i) }
  def db_check(checks)      = checks.find { |c| c[:name].match?(/database|connect/i) }
  def pgmq_check(checks)    = checks.find { |c| c[:name].match?(/pgmq|schema/i) }
  def queue_check(checks)   = checks.find { |c| c[:name].match?(/queue/i) && !c[:name].match?(/notify/i) }
  def notify_check(checks)  = checks.find { |c| c[:name].match?(/notify/i) }
  def process_check(checks) = checks.find { |c| c[:name].match?(/process|liveness|health/i) }
  def gid_check(checks)     = checks.find { |c| c[:name].match?(/globalid|allowlist/i) }
end
