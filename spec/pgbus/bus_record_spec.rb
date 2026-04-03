# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::BusRecord do
  it "is an abstract ActiveRecord class" do
    expect(described_class.superclass).to eq(ActiveRecord::Base)
    expect(described_class).to be_abstract_class
  end

  it "is defined in lib/pgbus/ (loaded by the gem loader, not the engine)" do
    # Verify that bus_record.rb lives under lib/pgbus/ where the main
    # Zeitwerk gem loader manages it — not under app/models/ which
    # depends on engine boot order.
    gem_root = File.expand_path("../..", __dir__)
    expect(File.exist?(File.join(gem_root, "lib/pgbus/bus_record.rb"))).to be(true)
    expect(File.exist?(File.join(gem_root, "app/models/pgbus/bus_record.rb"))).to be(false)
  end
end
