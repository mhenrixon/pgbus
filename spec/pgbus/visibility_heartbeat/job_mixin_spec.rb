# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::VisibilityHeartbeat::JobMixin do
  let(:job_class) do
    Class.new do
      include Pgbus::VisibilityHeartbeat::JobMixin
    end
  end

  it "follows the global setting by default" do
    expect(job_class.pgbus_visibility_heartbeat_enabled).to be_nil
  end

  it "opts a class out" do
    job_class.pgbus_visibility_heartbeat false

    expect(job_class.pgbus_visibility_heartbeat_enabled).to be(false)
  end

  it "opts a class in explicitly" do
    job_class.pgbus_visibility_heartbeat

    expect(job_class.pgbus_visibility_heartbeat_enabled).to be(true)
  end

  it "is inherited by subclasses and overridable per subclass" do
    job_class.pgbus_visibility_heartbeat false
    subclass = Class.new(job_class)
    override = Class.new(job_class) { pgbus_visibility_heartbeat true }

    expect(subclass.pgbus_visibility_heartbeat_enabled).to be(false)
    expect(override.pgbus_visibility_heartbeat_enabled).to be(true)
  end
end
