# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::PrimaryValidator do
  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  # A minimal PG::Connection double: exec("SELECT pg_is_in_recovery()") returns a
  # PG::Result-like object whose getvalue(0, 0) is the scripted "t"/"f" string.
  def build_conn(in_recovery:)
    result = Object.new
    value = in_recovery ? "t" : "f"
    result.define_singleton_method(:getvalue) { |_row, _col| value }

    conn = Object.new
    executed = []
    conn.define_singleton_method(:exec) do |sql|
      executed << sql
      result
    end
    conn.define_singleton_method(:executed) { executed }
    conn
  end

  describe ".validate_primary!" do
    context "when the connection is a primary (pg_is_in_recovery() => f)" do
      let(:conn) { build_conn(in_recovery: false) }

      it "returns the connection" do
        expect(described_class.validate_primary!(conn)).to be(conn)
      end

      it "runs SELECT pg_is_in_recovery()" do
        described_class.validate_primary!(conn)
        expect(conn.executed).to include("SELECT pg_is_in_recovery()")
      end

      it "does not raise" do
        expect { described_class.validate_primary!(conn) }.not_to raise_error
      end
    end

    context "when the connection is a read-only replica (pg_is_in_recovery() => t)" do
      let(:conn) { build_conn(in_recovery: true) }

      it "raises ReplicaConnectionError" do
        expect { described_class.validate_primary!(conn) }
          .to raise_error(Pgbus::Process::ReplicaConnectionError)
      end

      it "raises a subclass of Pgbus::Error" do
        expect(Pgbus::Process::ReplicaConnectionError.ancestors).to include(Pgbus::Error)
      end

      it "names the replica/recovery condition in the message" do
        expect { described_class.validate_primary!(conn) }
          .to raise_error(Pgbus::Process::ReplicaConnectionError, /replica|recovery/i)
      end
    end
  end
end
