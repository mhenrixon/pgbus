# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus install generator migration template" do # rubocop:disable RSpec/DescribeClass
  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/migration.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "uses enable_extension only conditionally, not as the sole install path" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema.install_sql")
      expect(content).to include("extension_available?")
    end

    it "references PgmqSchema for installation" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema")
    end

    it "includes pgmq_schema_mode detection" do
      content = File.read(template_path)
      expect(content).to include("pgmq_schema_mode")
    end
  end
end
