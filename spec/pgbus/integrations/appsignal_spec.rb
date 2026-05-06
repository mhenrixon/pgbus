# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus::Integrations::Appsignal" do
  let(:probes_class) do
    Class.new do
      class << self
        attr_accessor :registered, :unregistered
      end
      self.registered = []
      self.unregistered = []

      def self.register(name, probe)
        registered << [name, probe]
      end

      def self.unregister(name)
        unregistered << name
      end
    end
  end

  let(:appsignal_class) do
    klass = Class.new
    klass.define_singleton_method(:set_gauge) { |*args| args }
    klass.define_singleton_method(:increment_counter) { |*args| args }
    klass.define_singleton_method(:add_distribution_value) { |*args| args }
    klass.define_singleton_method(:set_error) { |*args| args }
    klass
  end

  let(:fake_transaction_class) do
    Class.new do
      def self.create(_kind)
        new
      end

      def self.complete_current!
        nil
      end

      # rubocop:disable Naming/AccessorMethodName
      def set_action_if_nil(_)
        nil
      end

      def set_queue_start(_)
        nil
      end
      # rubocop:enable Naming/AccessorMethodName

      def add_tags(_)
        nil
      end

      def add_params_if_nil
        nil
      end

      def set_error(_) # rubocop:disable Naming/AccessorMethodName
        nil
      end
    end
  end

  before do
    stub_const("Appsignal", appsignal_class)
    stub_const("Appsignal::Probes", probes_class)
    stub_const("Appsignal::Transaction", fake_transaction_class)
    stub_const("Appsignal::Transaction::BACKGROUND_JOB", "background_job")
    require "pgbus/integrations/appsignal"
    Pgbus::Integrations::Appsignal.reset!
  end

  after do
    Pgbus::Integrations::Appsignal.reset!
  end

  it "installs the subscriber and registers the probe" do
    Pgbus::Integrations::Appsignal.install!

    expect(Pgbus::Integrations::Appsignal.installed?).to be true
    expect(probes_class.registered.first&.first).to eq(:pgbus)
  end

  it "is idempotent" do
    Pgbus::Integrations::Appsignal.install!
    Pgbus::Integrations::Appsignal.install!

    expect(probes_class.registered.size).to eq(1)
  end

  it "skips probe registration when appsignal_probe_enabled is false" do
    Pgbus.configuration.appsignal_probe_enabled = false
    Pgbus::Integrations::Appsignal.install!

    expect(probes_class.registered).to be_empty
  ensure
    Pgbus.configuration.appsignal_probe_enabled = true
  end

  it "noop when ::Appsignal is undefined" do
    Pgbus::Integrations::Appsignal.reset!
    hide_const("Appsignal")

    expect(Pgbus::Integrations::Appsignal.install!).to be(false)
    expect(Pgbus::Integrations::Appsignal.installed?).to be(false)
  end
end
