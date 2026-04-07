# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::Registry do
  subject(:registry) { described_class.new }

  # Connections are treated as opaque duck-typed objects. The Registry only
  # reads #id and #stream_name. The real Pgbus::Web::Streamer::Connection
  # will arrive in Phase 2.2; until then, a Struct stand-in is sufficient
  # and makes the Registry's contract obvious.
  ConnStub = Struct.new(:id, :stream_name) unless defined?(ConnStub)

  def build_conn(id:, stream_name:)
    ConnStub.new(id, stream_name)
  end

  describe "#register" do
    it "adds a connection under its stream name" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      expect(registry.connections_for("chat")).to contain_exactly(conn)
    end

    it "indexes the connection by id" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      expect(registry.lookup("c1")).to eq(conn)
    end

    it "is idempotent for the same (id, stream) pair" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      registry.register(conn)
      expect(registry.connections_for("chat")).to contain_exactly(conn)
      expect(registry.size).to eq(1)
    end

    it "supports many connections on the same stream" do
      c1 = build_conn(id: "c1", stream_name: "chat")
      c2 = build_conn(id: "c2", stream_name: "chat")
      c3 = build_conn(id: "c3", stream_name: "chat")
      [c1, c2, c3].each { |c| registry.register(c) }
      expect(registry.connections_for("chat")).to contain_exactly(c1, c2, c3)
    end
  end

  describe "#unregister" do
    it "removes a connection by identity" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      registry.unregister(conn)
      expect(registry.connections_for("chat")).to be_empty
      expect(registry.lookup("c1")).to be_nil
    end

    it "is a no-op for an unknown connection" do
      conn = build_conn(id: "ghost", stream_name: "chat")
      expect { registry.unregister(conn) }.not_to raise_error
    end

    it "prunes empty stream entries so #streams reflects reality" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      registry.unregister(conn)
      expect(registry.streams).to be_empty
    end
  end

  describe "#connections_for" do
    it "returns a snapshot (mutating the snapshot does not affect the registry)" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      snapshot = registry.connections_for("chat")
      snapshot.clear
      expect(registry.connections_for("chat")).to contain_exactly(conn)
    end

    it "returns an empty array for unknown streams" do
      expect(registry.connections_for("unknown")).to eq([])
    end
  end

  describe "#streams" do
    it "returns the set of stream names currently serving at least one connection" do
      registry.register(build_conn(id: "a", stream_name: "chat"))
      registry.register(build_conn(id: "b", stream_name: "presence"))
      expect(registry.streams).to contain_exactly("chat", "presence")
    end

    it "returns a snapshot" do
      registry.register(build_conn(id: "a", stream_name: "chat"))
      snapshot = registry.streams
      snapshot.clear
      expect(registry.streams).to contain_exactly("chat")
    end
  end

  describe "#empty?" do
    it "returns true when a stream has no connections" do
      expect(registry.empty?("chat")).to be true
    end

    it "returns true after the last connection is removed" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      registry.unregister(conn)
      expect(registry.empty?("chat")).to be true
    end

    it "returns false when the stream has at least one connection" do
      registry.register(build_conn(id: "c1", stream_name: "chat"))
      expect(registry.empty?("chat")).to be false
    end
  end

  describe "#size" do
    it "returns total connection count across all streams" do
      registry.register(build_conn(id: "a", stream_name: "chat"))
      registry.register(build_conn(id: "b", stream_name: "chat"))
      registry.register(build_conn(id: "c", stream_name: "presence"))
      expect(registry.size).to eq(3)
    end
  end

  describe "#each_connection" do
    it "yields every registered connection across all streams" do
      c1 = build_conn(id: "a", stream_name: "chat")
      c2 = build_conn(id: "b", stream_name: "presence")
      registry.register(c1)
      registry.register(c2)

      yielded = []
      registry.each_connection { |c| yielded << c }
      expect(yielded).to contain_exactly(c1, c2)
    end

    it "iterates over a snapshot — unregistering during iteration does not affect the current pass" do
      c1 = build_conn(id: "a", stream_name: "chat")
      c2 = build_conn(id: "b", stream_name: "chat")
      registry.register(c1)
      registry.register(c2)

      yielded = []
      registry.each_connection do |c|
        yielded << c
        registry.unregister(c)
      end
      expect(yielded).to contain_exactly(c1, c2)
      expect(registry.size).to eq(0)
    end
  end

  describe "thread safety" do
    it "handles concurrent register/unregister without raising or losing connections" do
      # 100 threads, 50 connections each, half registering and half unregistering.
      # The final state must be deterministic: every registered connection is
      # either present or cleanly removed, no orphan entries.
      conns = Array.new(5_000) { |i| build_conn(id: "c#{i}", stream_name: "stream#{i % 10}") }

      # Register all connections in parallel
      threads = conns.each_slice(100).map do |batch|
        Thread.new { batch.each { |c| registry.register(c) } }
      end
      threads.each(&:join)

      expect(registry.size).to eq(5_000)

      # Now unregister half in parallel while reading the other half
      to_remove = conns.sample(2_500)
      remove_threads = to_remove.each_slice(100).map do |batch|
        Thread.new { batch.each { |c| registry.unregister(c) } }
      end
      # The block body is intentionally a no-op — the point of these threads
      # is to iterate concurrently with unregister calls and prove that
      # nothing raises or deadlocks.
      read_threads = Array.new(5) { Thread.new { registry.each_connection { |_c| :seen } } }
      (remove_threads + read_threads).each(&:join)

      expect(registry.size).to eq(2_500)
    end
  end
end
