# frozen_string_literal: true

require "spec_helper"

# Issue #369: the README doctor table drifted to 6 of 10 checks. Guard the
# public check names so a new Doctor::CHECKS entry fails CI until the README
# table is updated. Assertions are scoped to the `#### pgbus doctor` section
# so a matching row elsewhere cannot false-pass a stale doctor table.
RSpec.describe "README doctor check table drift (issue #369)" do # rubocop:disable RSpec/DescribeClass
  let(:readme) { File.read(File.expand_path("../../README.md", __dir__)) }
  let(:doctor_section) { readme[/^#### pgbus doctor\b.*?(?=^#### |\z)/m] }

  it "documents every Pgbus::Doctor::CHECKS name" do
    expect(doctor_section).not_to be_nil, "README is missing a '#### pgbus doctor' section"

    missing = Pgbus::Doctor::CHECKS.keys.reject { |name| doctor_section.include?("| #{name} |") }

    expect(missing).to be_empty,
                       "README doctor table is missing check(s): #{missing.join(", ")}. " \
                       "Update the table under '#### pgbus doctor' in README.md."
  end

  it "does not claim a stale check count in the doctor prose" do
    expect(doctor_section).not_to be_nil, "README is missing a '#### pgbus doctor' section"
    # "six checks" was the pre-#369 wording; keep the prose in sync with CHECKS.size.
    expect(doctor_section).not_to match(/\bruns?\s+six\s+checks\b/i)
    expect(doctor_section).to match(
      /\bruns?\s+#{Pgbus::Doctor::CHECKS.size}\s+checks\b/i
    )
  end
end
