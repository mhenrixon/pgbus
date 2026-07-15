# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DedicatedConnection do
  before { require "pg" }

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
      it "passes it straight to PG.connect" do
        captured = nil
        allow(PG).to receive(:connect) do |arg|
          captured = arg
          fake_conn
        end

        conn = described_class.connect("postgres://user@localhost:5432/app")

        expect(captured).to eq("postgres://user@localhost:5432/app")
        expect(conn).to be(fake_conn)
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

        expect(captured).to eq(host: "db.example", port: 5432, dbname: "app", user: "app")
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

        expect(captured).to eq(host: "db.example", dbname: "app")
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

      it "execs nothing for nil variables" do
        captured = nil
        allow(PG).to receive(:connect) do |**kwargs|
          captured = kwargs
          fake_conn
        end

        described_class.connect(dbname: "app", variables: nil)

        expect(captured).to eq(dbname: "app")
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
