# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::TableMaintenance do
  describe ".fillfactor_sql_for_queue" do
    subject(:sql) { described_class.fillfactor_sql_for_queue("pgbus_default") }

    it "returns ALTER TABLE for queue table with fillfactor" do
      expect(sql).to include("ALTER TABLE pgmq.q_pgbus_default SET (fillfactor = 70)")
    end

    it "does not alter the archive table" do
      expect(sql).not_to include("pgmq.a_pgbus_default")
    end
  end

  describe ".fillfactor_sql_for_all_queues" do
    subject(:sql) { described_class.fillfactor_sql_for_all_queues }

    it "returns a DO $$ block that iterates pgmq.meta" do
      expect(sql).to include("DO $$")
      expect(sql).to include("SELECT queue_name FROM pgmq.meta")
    end

    it "applies fillfactor to q_ tables only" do
      expect(sql).to include("pgmq.q_%I")
      expect(sql).to include("fillfactor = 70")
    end

    it "does not apply fillfactor to archive tables" do
      expect(sql).not_to include("pgmq.a_%I")
    end
  end

  describe ".vacuum_candidates" do
    let(:conn) { double("connection") }

    it "returns tables exceeding the bloat threshold" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_default",
          "n_dead_tup" => 500, "n_live_tup" => 1000 },
        { "schemaname" => "pgmq", "relname" => "a_pgbus_default",
          "n_dead_tup" => 10, "n_live_tup" => 5000 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to contain_exactly(
        hash_including(table: "pgmq.q_pgbus_default", dead_ratio: be > 0.1)
      )
    end

    it "returns empty array when no tables exceed threshold" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_default",
          "n_dead_tup" => 5, "n_live_tup" => 5000 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to be_empty
    end

    it "skips tables with zero total tuples" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_empty",
          "n_dead_tup" => 0, "n_live_tup" => 0 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to be_empty
    end
  end

  describe ".vacuum_sql" do
    it "generates VACUUM for the given table" do
      sql = described_class.vacuum_sql("pgmq.q_pgbus_default")
      expect(sql).to eq("VACUUM pgmq.q_pgbus_default")
    end
  end

  describe ".reindex_sql" do
    it "generates REINDEX TABLE CONCURRENTLY" do
      sql = described_class.reindex_sql("pgmq.q_pgbus_default")
      expect(sql).to eq("REINDEX TABLE CONCURRENTLY pgmq.q_pgbus_default")
    end
  end

  describe "FILLFACTOR" do
    it "is set to 70" do
      expect(described_class::FILLFACTOR).to eq(70)
    end

    it "leaves room for HOT updates (must be < 100)" do
      expect(described_class::FILLFACTOR).to be < 100
      expect(described_class::FILLFACTOR).to be >= 50
    end
  end

  describe "BLOAT_THRESHOLD" do
    it "defaults to 10% dead tuple ratio" do
      expect(described_class::BLOAT_THRESHOLD).to eq(0.1)
    end
  end
end
