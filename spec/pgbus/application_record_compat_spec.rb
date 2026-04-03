# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ApplicationRecord do
  it "inherits from BusRecord for backward compatibility" do
    expect(described_class.superclass).to eq(Pgbus::BusRecord)
  end

  it "is an abstract class" do
    expect(described_class).to be_abstract_class
  end

  it "allows subclasses to work like BusRecord subclasses" do
    klass = Class.new(described_class)
    expect(klass.ancestors).to include(Pgbus::BusRecord)
    expect(klass.ancestors).to include(ActiveRecord::Base)
  end
end
