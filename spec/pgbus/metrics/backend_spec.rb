# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Metrics::Backend do
  describe "the abstract interface" do
    subject(:backend) { described_class.new }

    it "raises NotImplementedError for #increment" do
      expect { backend.increment("x") }.to raise_error(NotImplementedError)
    end

    it "raises NotImplementedError for #gauge" do
      expect { backend.gauge("x", 1) }.to raise_error(NotImplementedError)
    end

    it "raises NotImplementedError for #histogram" do
      expect { backend.histogram("x", 1) }.to raise_error(NotImplementedError)
    end
  end

  describe Pgbus::Metrics::Backend::Null do
    subject(:backend) { described_class.new }

    it "no-ops #increment and returns nil" do
      expect(backend.increment("x", 5, { queue: "default" })).to be_nil
    end

    it "no-ops #gauge and returns nil" do
      expect(backend.gauge("x", 3)).to be_nil
    end

    it "no-ops #histogram and returns nil" do
      expect(backend.histogram("x", 3)).to be_nil
    end
  end

  describe ".resolve" do
    it "returns a Prometheus backend for :prometheus" do
      expect(Pgbus::Metrics.resolve_backend(:prometheus))
        .to be_a(Pgbus::Metrics::Backends::Prometheus)
    end

    it "returns a Statsd backend for :statsd" do
      expect(Pgbus::Metrics.resolve_backend(:statsd))
        .to be_a(Pgbus::Metrics::Backends::Statsd)
    end

    it "returns the instance unchanged when given a Backend instance" do
      instance = Pgbus::Metrics::Backend::Null.new
      expect(Pgbus::Metrics.resolve_backend(instance)).to equal(instance)
    end

    it "returns the Null backend for nil" do
      expect(Pgbus::Metrics.resolve_backend(nil))
        .to be_a(Pgbus::Metrics::Backend::Null)
    end

    it "raises ArgumentError for an unknown symbol" do
      expect { Pgbus::Metrics.resolve_backend(:bogus) }.to raise_error(ArgumentError)
    end
  end
end
