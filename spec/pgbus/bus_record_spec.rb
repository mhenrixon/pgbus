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

  describe ".disconnect_all_pools!" do
    let(:handler) { instance_double(ActiveRecord::ConnectionAdapters::ConnectionHandler) }
    let(:owned_pool) { double("pool", connection_class: described_class) }
    let(:foreign_pool) { double("pool", connection_class: ActiveRecord::Base) }

    before do
      allow(described_class).to receive(:connection_handler).and_return(handler)
      allow(handler).to receive(:connection_pool_list).with(:all).and_return([owned_pool, foreign_pool])
      allow(owned_pool).to receive(:disconnect!)
      allow(foreign_pool).to receive(:disconnect!)
    end

    it "disconnects every pool owned by BusRecord (all roles)" do
      described_class.disconnect_all_pools!
      expect(owned_pool).to have_received(:disconnect!)
    end

    it "never touches pools owned by other classes (primary-database setups)" do
      described_class.disconnect_all_pools!
      expect(foreign_pool).not_to have_received(:disconnect!)
    end

    it "is a no-op when no BusRecord-owned pool exists" do
      allow(handler).to receive(:connection_pool_list).with(:all).and_return([foreign_pool])
      expect { described_class.disconnect_all_pools! }.not_to raise_error
    end
  end
end
