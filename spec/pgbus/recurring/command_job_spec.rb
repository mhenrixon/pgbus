# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Recurring::CommandJob do
  it "inherits from ActiveJob::Base" do
    expect(described_class.ancestors).to include(ActiveJob::Base)
  end

  it "uses the default ActiveJob queue (no hardcoded queue)" do
    job = described_class.new("OldRecord.cleanup")
    expect(job.queue_name).to eq("default")
  end

  describe "#perform" do
    it "calls the method on the resolved constant" do
      stub_const("OldRecord", Class.new { def self.cleanup! = nil })
      allow(OldRecord).to receive(:cleanup!)

      described_class.new.perform("OldRecord.cleanup!")

      expect(OldRecord).to have_received(:cleanup!)
    end

    it "supports namespaced constants" do
      stub_const("Admin::Maintenance", Class.new { def self.run = nil })
      allow(Admin::Maintenance).to receive(:run)

      described_class.new.perform("Admin::Maintenance.run")

      expect(Admin::Maintenance).to have_received(:run)
    end

    it "rejects arbitrary Ruby expressions" do
      expect { described_class.new.perform("system('rm -rf /')") }
        .to raise_error(ArgumentError, /Unsafe recurring command/)
    end

    it "rejects eval-like injection attempts" do
      expect { described_class.new.perform("eval('puts 1')") }
        .to raise_error(ArgumentError, /Unsafe recurring command/)
    end

    it "rejects commands with arguments in parentheses" do
      expect { described_class.new.perform("Foo.bar(1)") }
        .to raise_error(ArgumentError, /Unsafe recurring command/)
    end

    it "rejects chained method calls" do
      expect { described_class.new.perform("Foo.bar.baz") }
        .to raise_error(ArgumentError, /Unsafe recurring command/)
    end

    it "raises when the constant does not exist" do
      expect { described_class.new.perform("NonexistentClass.run") }
        .to raise_error(ArgumentError, /Unknown class/)
    end
  end
end
