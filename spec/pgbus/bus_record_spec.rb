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

    before do
      allow(described_class).to receive(:connection_handler).and_return(handler)
      allow(handler).to receive(:connection_pool_list).with(:all).and_return([owned_pool, foreign_pool])
      allow(owned_pool).to receive(:disconnect!)
      allow(foreign_pool).to receive(:disconnect!)
    end

    shared_examples "pool ownership filtering" do
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

    # Rails >= 8.0: ConnectionPool exposes its owner via #connection_descriptor
    # (a ConnectionDescriptor whose #name is the owning class name) —
    # #connection_class no longer exists (issue #411).
    context "with descriptor-shaped pools (Rails >= 8.0)" do
      let(:owned_pool) { double("pool", connection_descriptor: double("descriptor", name: described_class.name)) }
      let(:foreign_pool) { double("pool", connection_descriptor: double("descriptor", name: "ActiveRecord::Base")) }

      it_behaves_like "pool ownership filtering"

      it "skips pools with a nil descriptor (null pools) without raising" do
        allow(handler).to receive(:connection_pool_list)
          .with(:all).and_return([double("pool", connection_descriptor: nil)])
        expect { described_class.disconnect_all_pools! }.not_to raise_error
      end
    end

    # Rails 7.1 / 7.2: ConnectionPool exposes its owner via #connection_class.
    context "with connection_class-shaped pools (Rails 7.x)" do
      let(:owned_pool) { double("pool", connection_class: described_class) }
      let(:foreign_pool) { double("pool", connection_class: ActiveRecord::Base) }

      it_behaves_like "pool ownership filtering"
    end

    # Regression guard for the #411 class of drift: verifying doubles only
    # allow stubbing methods the running Rails' ConnectionPool actually
    # defines, so this context fails on any matrix leg whose ownership API
    # the implementation no longer reads. The plain doubles above cannot
    # catch that — they happily stub methods that don't exist.
    context "with verified doubles of the running Rails ConnectionPool API" do
      if ActiveRecord::ConnectionAdapters::ConnectionPool.method_defined?(:connection_descriptor)
        let(:owned_pool) { verified_pool(described_class.name) }
        let(:foreign_pool) { verified_pool("ActiveRecord::Base") }

        def verified_pool(owner_name)
          descriptor = instance_double(ActiveRecord::ConnectionAdapters::ConnectionHandler::ConnectionDescriptor,
                                       name: owner_name)
          instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool, connection_descriptor: descriptor)
        end
      else
        let(:owned_pool) do
          instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool, connection_class: described_class)
        end
        let(:foreign_pool) do
          instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool, connection_class: ActiveRecord::Base)
        end
      end

      it_behaves_like "pool ownership filtering"
    end
  end
end
