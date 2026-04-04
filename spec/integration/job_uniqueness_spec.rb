# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job uniqueness (integration)", :integration do
  before do
    Pgbus::UniquenessKey.delete_all
  end

  describe "UniquenessKey acquire/release" do
    it "prevents duplicate acquisition when lock is held" do
      acquired = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      expect(acquired).to be true

      duplicate = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 2)
      expect(duplicate).to be false
    end

    it "allows acquisition after lock is released" do
      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      Pgbus::UniquenessKey.release!("unique-order-42")

      acquired = Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 3)
      expect(acquired).to be true
    end

    it "checks lock status correctly" do
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be false

      Pgbus::UniquenessKey.acquire!("unique-order-42", queue_name: "default", msg_id: 1)
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be true

      Pgbus::UniquenessKey.release!("unique-order-42")
      expect(Pgbus::UniquenessKey.locked?("unique-order-42")).to be false
    end
  end

  describe "concurrent lock acquisition" do
    it "only one thread wins the lock" do
      results = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(5)

      threads = 5.times.map do |i|
        Thread.new do
          barrier.wait
          acquired = Pgbus::UniquenessKey.acquire!(
            "race-key",
            queue_name: "default",
            msg_id: i
          )
          results << acquired
        end
      end

      threads.each(&:join)

      winners = results.count(true)
      losers = results.count(false)

      expect(winners).to eq(1)
      expect(losers).to eq(4)
    end
  end
end
