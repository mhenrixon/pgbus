# frozen_string_literal: true

require "spec_helper"
# Load pg at spec-LOAD time, not inside an example: other spec files
# conditionally stub_const("PG", Module.new) when pg isn't loaded, and a
# require that fires inside such a stub window loads the real gem into the
# throwaway stub module — leaving PG undefined (but marked loaded) for the
# rest of the process. Loading here runs before any example, on every seed.
require "pg"

RSpec.describe Pgbus::DedicatedConnection do
  let(:fake_conn) do
    Class.new do
      attr_reader :executed

      def initialize
        @executed = []
      end

      def exec(sql)
        @executed << sql
        nil
      end
    end.new
  end

  describe ".connect" do
    context "with a String conninfo/URL" do
      it "passes it to PG.connect with the census application name appended" do
        captured = nil
        allow(PG).to receive(:connect) do |arg|
          captured = arg
          fake_conn
        end

        conn = described_class.connect("postgres://user@localhost:5432/app")

        expect(captured).to eq("postgres://user@localhost:5432/app?fallback_application_name=pgbus-listen")
        expect(conn).to be(fake_conn)
      end
    end

    # Every dedicated LISTEN connection is stamped with a stable
    # application_name so operators (and the #381 connection-budget work) can
    # count them in pg_stat_activity. fallback_application_name is used so an
    # explicit application_name in the URL/hash — or PGAPPNAME — always wins.
    context "when stamping the census application_name (issue #381)" do
      let(:captured) { {} }

      before do
        allow(PG).to receive(:connect) do |*args, **kwargs|
          captured[:args] = args
          captured[:kwargs] = kwargs
          fake_conn
        end
      end

      it "appends with & when the URL already has a query string" do
        described_class.connect("postgres://u@h/db?sslmode=require")

        expect(captured[:args].first)
          .to eq("postgres://u@h/db?sslmode=require&fallback_application_name=pgbus-listen")
      end

      it "appends a space-separated keyword to a key=value conninfo string" do
        described_class.connect("host=db.example dbname=app")

        expect(captured[:args].first).to eq("host=db.example dbname=app fallback_application_name=pgbus-listen")
      end

      it "leaves a String untouched when it already sets an application_name" do
        described_class.connect("postgres://u@h/db?application_name=custom")

        expect(captured[:args].first).to eq("postgres://u@h/db?application_name=custom")
      end

      it "merges fallback_application_name into a Hash" do
        described_class.connect(host: "db.example", dbname: "app")

        expect(captured[:kwargs]).to eq(host: "db.example", dbname: "app",
                                        fallback_application_name: "pgbus-listen")
      end

      it "does not override an explicit application_name in a Hash" do
        described_class.connect(host: "db.example", dbname: "app", application_name: "custom")

        expect(captured[:kwargs]).to eq(host: "db.example", dbname: "app", application_name: "custom")
      end

      it "does not override an explicit fallback_application_name in a Hash" do
        described_class.connect(host: "db.example", dbname: "app", fallback_application_name: "mine")

        expect(captured[:kwargs]).to eq(host: "db.example", dbname: "app", fallback_application_name: "mine")
      end
    end

    context "with a Hash of libpq options (no :variables)" do
      it "connects with the same keys and execs nothing" do
        captured = nil
        allow(PG).to receive(:connect) do |**kwargs|
          captured = kwargs
          fake_conn
        end

        conn = described_class.connect(host: "db.example", port: 5432, dbname: "app", user: "app")

        expect(captured).to eq(host: "db.example", port: 5432, dbname: "app", user: "app",
                               fallback_application_name: "pgbus-listen")
        expect(conn.executed).to be_empty
      end
    end

    context "with a Hash carrying :variables (:session GUC mode)" do
      # Configuration#forward_connection_variables leaves the database.yml
      # `variables:` hash on the options in :session mode; :variables is not
      # a libpq keyword, so passing it through raises
      # `PG::Error: invalid connection option "variables"` (issue #352).
      it "strips :variables before PG.connect" do
        captured = nil
        allow(PG).to receive(:connect) do |**kwargs|
          captured = kwargs
          fake_conn
        end

        described_class.connect(host: "db.example", dbname: "app",
                                variables: { statement_timeout: "10s" })

        expect(captured).to eq(host: "db.example", dbname: "app",
                               fallback_application_name: "pgbus-listen")
      end

      it "applies each GUC via post-connect SET, in order" do
        allow(PG).to receive(:connect).and_return(fake_conn)

        described_class.connect(host: "db.example", dbname: "app",
                                variables: { statement_timeout: "10s", timezone: "UTC" })

        expect(fake_conn.executed).to eq(["SET statement_timeout = '10s'", "SET timezone = 'UTC'"])
      end

      it "returns the connection" do
        allow(PG).to receive(:connect).and_return(fake_conn)

        conn = described_class.connect(dbname: "app", variables: { timezone: "UTC" })

        expect(conn).to be(fake_conn)
      end

      it "execs nothing for empty variables" do
        allow(PG).to receive(:connect).and_return(fake_conn)

        described_class.connect(dbname: "app", variables: {})

        expect(fake_conn.executed).to be_empty
      end

      context "when a SET raises after the connect succeeded" do
        # The reconnect loops (NotifyListener#reconnect!, streamer Listener)
        # retry build_connection on a 500ms backoff. Without closing here, a
        # deterministically failing SET (e.g. a bogus GUC name in variables:)
        # would orphan one server connection per retry until PostgreSQL
        # exhausts max_connections.
        let(:failing_conn) do
          Class.new do
            attr_reader :close_count

            def initialize
              @close_count = 0
            end

            def exec(_sql)
              raise StandardError, 'unrecognized configuration parameter "bogus"'
            end

            def close
              @close_count += 1
            end
          end.new
        end

        it "closes the freshly opened connection before re-raising" do
          allow(PG).to receive(:connect).and_return(failing_conn)

          expect do
            described_class.connect(dbname: "app", variables: { bogus: "x" })
          end.to raise_error(StandardError, /unrecognized configuration parameter/)

          expect(failing_conn.close_count).to eq(1)
        end

        it "re-raises the SET error even when close itself fails" do
          allow(failing_conn).to receive(:close).and_raise(StandardError.new("close failed"))
          allow(PG).to receive(:connect).and_return(failing_conn)

          expect do
            described_class.connect(dbname: "app", variables: { bogus: "x" })
          end.to raise_error(StandardError, /unrecognized configuration parameter/)
        end
      end

      it "execs nothing for nil variables" do
        captured = nil
        allow(PG).to receive(:connect) do |**kwargs|
          captured = kwargs
          fake_conn
        end

        described_class.connect(dbname: "app", variables: nil)

        expect(captured).to eq(dbname: "app", fallback_application_name: "pgbus-listen")
        expect(fake_conn.executed).to be_empty
      end
    end

    context "with anything else (e.g. a Proc)" do
      it "raises a ConfigurationError naming the class" do
        expect do
          described_class.connect(-> { :not_a_conn })
        end.to raise_error(Pgbus::ConfigurationError, /Proc/)
      end
    end
  end
end
