# frozen_string_literal: true

require "spec_helper"

# Pins the 1.0 error-hierarchy contract (issue #282). Two policies:
#   1. Operational errors descend from Pgbus::Error so `rescue Pgbus::Error`
#      catches every pgbus-raised operational failure.
#   2. Argument-shape errors (malformed caller input) stay ArgumentError
#      subclasses by design — that's what ArgumentError means.
RSpec.describe Pgbus::Error do
  # Generators live outside Zeitwerk (lib/pgbus.rb ignores lib/pgbus/generators),
  # so ConfigConverter isn't autoloaded — require it so its Error is defined.
  # The rest resolve lazily via const_get below.
  before { require "pgbus/generators/config_converter" }

  describe "operational errors descend from Pgbus::Error" do
    [
      "Pgbus::ConfigurationError",
      "Pgbus::SerializationError",
      "Pgbus::QueueNotFoundError",
      "Pgbus::DeadLetterError",
      "Pgbus::ConcurrencyLimitExceeded",
      "Pgbus::JobNotUnique",
      "Pgbus::SchemaNotReady",
      "Pgbus::ReadTimeoutError",
      "Pgbus::ConnectionCircuitOpenError",
      "Pgbus::EnqueueError",
      "Pgbus::ExecutionPoolError",
      "Pgbus::Process::ReplicaConnectionError",
      "Pgbus::PgmqSchema::VersionNotFoundError",
      "Pgbus::Streams::SignedName::InvalidSignedName",
      "Pgbus::Streams::SignedName::MissingSecret",
      "Pgbus::Generators::ConfigConverter::Error"
    ].each do |const_name|
      it "#{const_name} < Pgbus::Error" do
        klass = Object.const_get(const_name)
        expect(klass).to be < described_class
      end
    end
  end

  describe "argument-shape errors stay ArgumentError subclasses (policy pin)" do
    [
      "Pgbus::Configuration::CapsuleDSL::ParseError",
      "Pgbus::Streams::Cursor::InvalidCursor",
      "Pgbus::Streams::StreamNameTooLong"
    ].each do |const_name|
      it "#{const_name} < ArgumentError and NOT < Pgbus::Error" do
        klass = Object.const_get(const_name)
        expect(klass).to be < ArgumentError
        expect(klass).not_to be < described_class
      end
    end
  end

  it "Pgbus::Error is a StandardError (rescuable by bare rescue)" do
    expect(described_class).to be < StandardError
  end
end
