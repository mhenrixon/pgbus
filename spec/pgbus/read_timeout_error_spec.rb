# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ReadTimeoutError do
  it "inherits from Pgbus::Error" do
    expect(described_class.ancestors).to include(Pgbus::Error)
  end

  it "inherits from StandardError" do
    expect(described_class.ancestors).to include(StandardError)
  end
end
