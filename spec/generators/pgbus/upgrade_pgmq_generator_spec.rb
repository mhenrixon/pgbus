# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus upgrade PGMQ generator migration template" do # rubocop:disable RSpec/DescribeClass
  describe "upgrade migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/upgrade_pgmq.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "drops existing pgmq functions before re-creating" do
      content = File.read(template_path)
      expect(content).to include("drop_pgmq_functions_sql")
    end

    it "uses PgmqSchema for the new SQL" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema")
    end

    it "tracks the version in pgbus_pgmq_schema_versions" do
      content = File.read(template_path)
      expect(content).to include("pgbus_pgmq_schema_versions")
    end

    it "records the install method as upgrade" do
      content = File.read(template_path)
      expect(content).to include("'upgrade'")
    end

    it "raises on down migration" do
      content = File.read(template_path)
      expect(content).to include("IrreversibleMigration")
    end
  end
end
