# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing/minitest"

# Verify that Minitest module can be loaded and includes the right methods.
# We don't actually run Minitest here — we just verify the module structure
# is correct and mixable.
RSpec.describe Pgbus::Testing::MinitestHelpers do
  it "includes assertion methods from Assertions" do
    expect(described_class.instance_methods).to include(:assert_pgbus_published)
    expect(described_class.instance_methods).to include(:assert_no_pgbus_published)
    expect(described_class.instance_methods).to include(:pgbus_published_events)
    expect(described_class.instance_methods).to include(:perform_published_events)
  end

  it "includes setup/teardown hooks" do
    expect(described_class.instance_methods).to include(:before_setup)
  end
end
