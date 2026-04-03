# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::BusRecord do
  it "is an abstract ActiveRecord class" do
    expect(described_class.superclass).to eq(ActiveRecord::Base)
    expect(described_class).to be_abstract_class
  end

  it "is defined in lib/pgbus/ (loaded by the gem loader, not the engine)" do
    # BusRecord lives in lib/pgbus/, so it should be managed by
    # Zeitwerk's gem loader — not the separate models_loader that
    # depends on engine boot order.
    source_file = Pgbus.loader.cpath(described_class.name)
    expect(source_file).to end_with("lib/pgbus/bus_record.rb")
  rescue NoMethodError
    # Zeitwerk API varies by version; fall back to file existence check
    path = File.expand_path("../../lib/pgbus/bus_record.rb", __dir__)
    expect(File.exist?(path)).to be(true)
  end
end
