# frozen_string_literal: true

require "spec_helper"
require "pgbus/streams/presence"

RSpec.describe Pgbus::Streams::Presence do
  subject(:presence) { described_class.new(stream) }

  let(:stream) { double("Pgbus::Streams::Stream", name: "room:42") }
  let(:raw_connection) { double("PG::Connection") }

  let(:ar_connection) { double("ActiveRecord::ConnectionAdapters::Connection", raw_connection: raw_connection) }

  before do
    # Force the single-database path (Pgbus.configuration.connects_to == nil)
    # so the spec doesn't require BusRecord to be connected. The multi-DB
    # branch is a one-line delegation to BusRecord.connection.raw_connection
    # and is exercised by spec/integration/streams/presence_spec.rb.
    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    stub_const("ActiveRecord::Base", Class.new)
    allow(ActiveRecord::Base).to receive(:connection).and_return(ar_connection)
  end

  describe "#join" do
    let(:upsert_row) do
      {
        "member_id" => "7",
        "metadata" => '{"name":"Alice"}',
        "joined_at" => "2026-04-09T00:00:00Z",
        "last_seen_at" => "2026-04-09T00:00:00Z"
      }
    end

    before do
      allow(raw_connection).to receive(:exec_params).and_return([upsert_row])
    end

    it "upserts the member and returns a hash with parsed metadata" do
      result = presence.join(member_id: 7, metadata: { name: "Alice" })

      expect(result["id"]).to eq("7")
      expect(result["metadata"]).to eq({ "name" => "Alice" })
    end

    it "coerces member_id to string before inserting" do
      presence.join(member_id: 7)

      expect(raw_connection).to have_received(:exec_params).with(
        anything,
        ["room:42", "7", "{}"]
      )
    end

    it "generates JSON for metadata even when empty" do
      presence.join(member_id: "7")

      expect(raw_connection).to have_received(:exec_params).with(
        anything,
        ["room:42", "7", "{}"]
      )
    end

    it "broadcasts the yielded HTML when the block returns a non-empty string" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { |_record| "<turbo-stream>join</turbo-stream>" }

      expect(stream).to have_received(:broadcast).with("<turbo-stream>join</turbo-stream>")
    end

    it "skips broadcast when the block returns an empty string" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { "" }

      expect(stream).not_to have_received(:broadcast)
    end

    it "skips broadcast when the block returns nil" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { nil }

      expect(stream).not_to have_received(:broadcast)
    end
  end

  describe "#leave" do
    context "when the member exists" do
      let(:deleted_row) do
        {
          "member_id" => "7",
          "metadata" => '{"name":"Alice"}',
          "joined_at" => "2026-04-09T00:00:00Z",
          "last_seen_at" => "2026-04-09T00:05:00Z"
        }
      end

      before do
        allow(raw_connection).to receive(:exec_params).and_return([deleted_row])
      end

      it "returns the deleted record" do
        result = presence.leave(member_id: "7")
        expect(result["id"]).to eq("7")
        expect(result["metadata"]).to eq({ "name" => "Alice" })
      end

      it "broadcasts the yielded HTML" do
        allow(stream).to receive(:broadcast)
        presence.leave(member_id: "7") { "<turbo-stream>leave</turbo-stream>" }

        expect(stream).to have_received(:broadcast).with("<turbo-stream>leave</turbo-stream>")
      end
    end

    context "when the member does not exist" do
      before { allow(raw_connection).to receive(:exec_params).and_return([]) }

      it "returns nil" do
        expect(presence.leave(member_id: "missing")).to be_nil
      end

      it "does not yield the block" do
        yielded = false
        presence.leave(member_id: "missing") { |_| yielded = true }
        expect(yielded).to be false
      end

      it "does not broadcast" do
        allow(stream).to receive(:broadcast)
        presence.leave(member_id: "missing") { "<turbo-stream>nope</turbo-stream>" }
        expect(stream).not_to have_received(:broadcast)
      end
    end
  end

  describe "#touch" do
    it "refreshes last_seen_at for the given member" do
      allow(raw_connection).to receive(:exec_params)

      presence.touch(member_id: 42)

      expect(raw_connection).to have_received(:exec_params).with(
        a_string_matching(/UPDATE pgbus_presence_members.*SET last_seen_at = NOW\(\)/m),
        ["room:42", "42"]
      )
    end
  end

  describe "#members" do
    context "with well-formed metadata" do
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => '{"name":"Alice"}',
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    },
                                                                    {
                                                                      "member_id" => "2",
                                                                      "metadata" => '{"name":"Bob"}',
                                                                      "joined_at" => "2026-04-09T00:01:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:01:00Z"
                                                                    }
                                                                  ])
      end

      it "returns an array of hashes with parsed metadata" do
        result = presence.members
        expect(result.size).to eq(2)
        expect(result[0]["id"]).to eq("1")
        expect(result[0]["metadata"]).to eq({ "name" => "Alice" })
        expect(result[1]["id"]).to eq("2")
      end
    end

    context "with corrupt JSON metadata" do
      # parse_metadata must not crash the whole members() call on one bad
      # row — production data can have legacy / manually-edited entries.
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => "{not valid json",
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    }
                                                                  ])
      end

      it "falls back to an empty hash without raising" do
        result = presence.members
        expect(result.size).to eq(1)
        expect(result[0]["metadata"]).to eq({})
      end

      it "logs the failure at debug level with the raw metadata truncated to 200 chars" do
        # Inject a metadata blob much longer than the 200-char cap so
        # we can distinguish "truncated" from "not truncated". The
        # production code does value.to_s[0, 200], then interpolates
        # via .inspect — the logged string contains the first 200
        # source characters and must NOT contain characters past 200.
        long_invalid = "{#{"x" * 500}"
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => long_invalid,
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    }
                                                                  ])

        logged_message = nil
        fake_logger = double("logger")
        allow(fake_logger).to receive(:debug) { |&blk| logged_message = blk&.call }
        allow(Pgbus).to receive(:logger).and_return(fake_logger)

        presence.members

        expect(fake_logger).to have_received(:debug)
        expect(logged_message).to include("parse_metadata failed")
        # First 200 chars are preserved in the log line
        expect(logged_message).to include(long_invalid[0, 200])
        # Characters past the 200-char boundary are dropped
        tail = long_invalid[200..]
        expect(logged_message).not_to include(tail)
      end
    end

    context "with nil or empty metadata" do
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    { "member_id" => "1", "metadata" => nil,
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z" },
                                                                    { "member_id" => "2", "metadata" => "",
                                                                      "joined_at" => "2026-04-09T00:01:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:01:00Z" }
                                                                  ])
      end

      it "returns empty-hash metadata for both without raising" do
        result = presence.members
        expect(result[0]["metadata"]).to eq({})
        expect(result[1]["metadata"]).to eq({})
      end
    end
  end

  describe "#count" do
    it "returns the integer count of members" do
      allow(raw_connection).to receive(:exec_params).and_return([{ "n" => "5" }])
      expect(presence.count).to eq(5)
    end
  end

  describe "#sweep!" do
    it "deletes expired members and returns the number of rows deleted" do
      expired = Time.now - 300
      allow(raw_connection).to receive(:exec_params).and_return([
                                                                  { "member_id" => "1" },
                                                                  { "member_id" => "2" }
                                                                ])

      result = presence.sweep!(older_than: expired)

      expect(result).to eq(2)
      expect(raw_connection).to have_received(:exec_params).with(
        a_string_matching(/DELETE FROM pgbus_presence_members.*RETURNING/m),
        ["room:42", expired]
      )
    end

    it "returns 0 when nothing to sweep" do
      allow(raw_connection).to receive(:exec_params).and_return([])
      expect(presence.sweep!(older_than: Time.now)).to eq(0)
    end
  end

  describe "connection resolution" do
    it "raises ConfigurationError when ActiveRecord is not loaded" do
      hide_const("ActiveRecord::Base")

      expect do
        presence.count
      end.to raise_error(Pgbus::ConfigurationError, /requires ActiveRecord/)
    end

    # The connects_to branch (Pgbus::BusRecord.connection.raw_connection)
    # is exercised by spec/integration/streams/presence_spec.rb against
    # a real DB — it's a 3-line delegation not worth stubbing AR's full
    # internals for here.
  end
end
