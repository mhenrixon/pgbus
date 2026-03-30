# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Concurrency::BlockedExecution do
  let(:connection) { double("AR::Connection") }

  before do
    stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
  end

  describe ".insert" do
    it "inserts a blocked execution row" do
      allow(connection).to receive(:exec_query).and_return([])

      described_class.insert(
        concurrency_key: "TestJob-42",
        queue_name: "default",
        payload: { "job_class" => "TestJob", "arguments" => [42] },
        priority: 0,
        duration: 900
      )

      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/INSERT INTO pgbus_blocked_executions/),
        "Pgbus Blocked Insert",
        array_including("TestJob-42", "default", a_string_matching(/"job_class"/), 0, an_instance_of(Time))
      )
    end
  end

  describe ".release_next" do
    it "returns the next blocked execution and deletes it" do
      row = { "queue_name" => "default", "payload" => '{"job_class":"TestJob","arguments":[42]}' }
      result = double("Result", first: row)
      allow(connection).to receive(:exec_query).and_return(result)

      released = described_class.release_next("TestJob-42")
      expect(released[:queue_name]).to eq("default")
      expect(released[:payload]["job_class"]).to eq("TestJob")
    end

    it "returns nil when no blocked executions exist" do
      result = double("Result", first: nil)
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.release_next("TestJob-42")).to be_nil
    end

    it "uses FOR UPDATE SKIP LOCKED to avoid contention" do
      result = double("Result", first: nil)
      allow(connection).to receive(:exec_query).and_return(result)

      described_class.release_next("TestJob-42")
      expect(connection).to have_received(:exec_query).with(
        a_string_matching(/FOR UPDATE SKIP LOCKED/),
        anything,
        anything
      )
    end
  end

  describe ".expire_stale" do
    it "deletes expired blocked executions" do
      result = double("Result", to_a: [{ "id" => 1 }, { "id" => 2 }])
      allow(connection).to receive(:exec_query).and_return(result)

      count = described_class.expire_stale
      expect(count).to eq(2)
    end
  end

  describe ".count_for" do
    it "returns the count of blocked executions for a key" do
      result = double("Result", first: { "cnt" => "5" })
      allow(connection).to receive(:exec_query).and_return(result)

      expect(described_class.count_for("TestJob-42")).to eq(5)
    end
  end

  context "when ActiveRecord is not defined" do
    before { hide_const("ActiveRecord") }

    it "release_next returns nil" do
      expect(described_class.release_next("key")).to be_nil
    end
  end
end
