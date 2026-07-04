# frozen_string_literal: true

# Shared wiring contract for every pgbus migration generator: it must be a
# Rails generator, mix in the shared MigrationPath routing, expose an opt-in
# --database flag, and carry a meaningful description. Pass the description
# matcher (a Regexp) the generator under test should satisfy.
RSpec.shared_examples "a pgbus generator" do |desc_matcher|
  it "is a Rails::Generators::Base subclass" do
    expect(described_class.ancestors).to include(Rails::Generators::Base)
  end

  it "mixes in the shared MigrationPath path logic" do
    expect(described_class.ancestors).to include(Pgbus::Generators::MigrationPath)
  end

  it "exposes a --database option defaulting to nil" do
    option = described_class.class_options[:database]
    expect(option).not_to be_nil
    expect(option.default).to be_nil
  end

  it "has a matching description" do
    expect(described_class.desc).to match(desc_matcher)
  end
end
