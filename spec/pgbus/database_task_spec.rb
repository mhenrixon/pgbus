# frozen_string_literal: true

require "spec_helper"
require "rake"

RSpec.describe Pgbus, ".database_task?" do
  def stub_rake_tasks(tasks)
    application = instance_double(Rake::Application, top_level_tasks: tasks)
    allow(Rake).to receive(:application).and_return(application)
  end

  it "returns true for db: tasks" do
    stub_rake_tasks(["db:test:purge"])
    expect(described_class.database_task?).to be(true)
  end

  it "returns true for db:migrate" do
    stub_rake_tasks(["db:migrate"])
    expect(described_class.database_task?).to be(true)
  end

  it "returns true for assets:precompile" do
    stub_rake_tasks(["assets:precompile"])
    expect(described_class.database_task?).to be(true)
  end

  it "returns true when a schema task is mixed with other tasks" do
    stub_rake_tasks(["some:task", "db:schema:load"])
    expect(described_class.database_task?).to be(true)
  end

  it "returns false for unrelated tasks" do
    stub_rake_tasks(["pgbus:doctor"])
    expect(described_class.database_task?).to be(false)
  end

  it "returns false with no top-level tasks" do
    stub_rake_tasks([])
    expect(described_class.database_task?).to be(false)
  end

  it "does not match a prefix embedded mid-name" do
    stub_rake_tasks(["mydb:refresh"])
    expect(described_class.database_task?).to be(false)
  end

  it "returns false (and logs at debug) when detection raises" do
    allow(Rake).to receive(:application).and_raise(RuntimeError, "boom")
    allow(described_class.logger).to receive(:debug)

    expect(described_class.database_task?).to be(false)
    expect(described_class.logger).to have_received(:debug)
  end
end
