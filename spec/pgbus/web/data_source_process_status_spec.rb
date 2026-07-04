# frozen_string_literal: true

require "spec_helper"
require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource do
  describe "#derive_process_status" do
    let(:mock_client) { build_mock_client }
    let(:data_source) { described_class.new(client: mock_client) }

    after do
      Pgbus.configuration.stall_threshold = 90
      Pgbus.configuration.dispatch_interval = 1.0
      Pgbus.configuration.recurring_schedule_interval = 1.0
      Pgbus.configuration.polling_interval = 0.1
      Pgbus.configuration.worker_notify_wakeup = nil
    end

    describe "#derive_process_status (private)" do
      context "when process is stale" do
        it "returns :stale" do
          result = data_source.send(:derive_process_status, true, {}, "worker")
          expect(result).to eq(:stale)
        end
      end

      context "when process is a healthy worker with recent loop_tick_at" do
        it "returns :healthy" do
          metadata = { "loop_tick_at" => Time.now.to_f }
          result = data_source.send(:derive_process_status, false, metadata, "worker")
          expect(result).to eq(:healthy)
        end
      end

      context "when process is a worker with stale loop_tick_at" do
        it "returns :stalled" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 120) }
          result = data_source.send(:derive_process_status, false, metadata, "worker")
          expect(result).to eq(:stalled)
        end
      end

      context "when process is a dispatcher with stale loop_tick_at" do
        it "returns :stalled" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 120) }
          result = data_source.send(:derive_process_status, false, metadata, "dispatcher")
          expect(result).to eq(:stalled)
        end
      end

      context "when process is a dispatcher with fresh loop_tick_at" do
        it "returns :healthy" do
          metadata = { "loop_tick_at" => Time.now.to_f }
          result = data_source.send(:derive_process_status, false, metadata, "dispatcher")
          expect(result).to eq(:healthy)
        end
      end

      context "when dispatcher beacon is stale only by its own sleep interval" do
        before { Pgbus.configuration.dispatch_interval = 5.0 }

        it "stays :healthy within stall_threshold + dispatch_interval" do
          # 92s old: past the 90s worker threshold but under 90 + 5 = 95s.
          metadata = { "loop_tick_at" => (Time.now.to_f - 92) }
          result = data_source.send(:derive_process_status, false, metadata, "dispatcher")
          expect(result).to eq(:healthy)
        end
      end

      context "when dispatcher has no loop_tick_at in metadata" do
        it "returns :healthy (older process during a rolling deploy)" do
          result = data_source.send(:derive_process_status, false, { "pid" => 1 }, "dispatcher")
          expect(result).to eq(:healthy)
        end
      end

      context "when process is a scheduler with stale loop_tick_at" do
        it "returns :stalled" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 120) }
          result = data_source.send(:derive_process_status, false, metadata, "scheduler")
          expect(result).to eq(:stalled)
        end
      end

      context "when process is a scheduler with fresh loop_tick_at" do
        it "returns :healthy" do
          metadata = { "loop_tick_at" => Time.now.to_f }
          result = data_source.send(:derive_process_status, false, metadata, "scheduler")
          expect(result).to eq(:healthy)
        end
      end

      context "when scheduler beacon is stale only by its own sleep interval" do
        before { Pgbus.configuration.recurring_schedule_interval = 5.0 }

        it "stays :healthy within stall_threshold + recurring_schedule_interval" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 92) }
          result = data_source.send(:derive_process_status, false, metadata, "scheduler")
          expect(result).to eq(:healthy)
        end
      end

      context "when scheduler has no loop_tick_at in metadata" do
        it "returns :healthy (older process during a rolling deploy)" do
          result = data_source.send(:derive_process_status, false, { "pid" => 1 }, "scheduler")
          expect(result).to eq(:healthy)
        end
      end

      # Consumer parity (issue #274). The consumer's empty-read wait can reach
      # NOTIFY_FALLBACK_POLL_SECONDS (15s) when a live listener drives wake-up,
      # so its beacon is naturally staler than a worker's. The threshold widens
      # by the consumer's max wait so a healthy idle consumer isn't flagged.
      context "when process is a consumer with stale loop_tick_at" do
        before { Pgbus.configuration.worker_notify_wakeup = false }
        after { Pgbus.configuration.worker_notify_wakeup = nil }

        it "returns :stalled well past the widened threshold" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 300) }
          result = data_source.send(:derive_process_status, false, metadata, "consumer")
          expect(result).to eq(:stalled)
        end
      end

      context "when process is a consumer with fresh loop_tick_at" do
        it "returns :healthy" do
          metadata = { "loop_tick_at" => Time.now.to_f }
          result = data_source.send(:derive_process_status, false, metadata, "consumer")
          expect(result).to eq(:healthy)
        end
      end

      context "when a NOTIFY-driven consumer beacon is stale only by its poll ceiling" do
        before { Pgbus.configuration.worker_notify_wakeup = true }
        after { Pgbus.configuration.worker_notify_wakeup = nil }

        it "stays :healthy within stall_threshold + NOTIFY_FALLBACK_POLL_SECONDS" do
          # 100s old: past the 90s worker threshold but under 90 + 15 = 105s.
          metadata = { "loop_tick_at" => (Time.now.to_f - 100) }
          result = data_source.send(:derive_process_status, false, metadata, "consumer")
          expect(result).to eq(:healthy)
        end
      end

      context "when a polling-only consumer beacon is stale only by its poll interval" do
        before do
          Pgbus.configuration.worker_notify_wakeup = false
          Pgbus.configuration.polling_interval = 5.0
        end

        after do
          Pgbus.configuration.worker_notify_wakeup = nil
          Pgbus.configuration.polling_interval = 0.1
        end

        it "stays :healthy within stall_threshold + polling_interval" do
          # 92s old: past 90s but under 90 + 5 = 95s (notify off, so no 15s ceiling).
          metadata = { "loop_tick_at" => (Time.now.to_f - 92) }
          result = data_source.send(:derive_process_status, false, metadata, "consumer")
          expect(result).to eq(:healthy)
        end
      end

      context "when consumer has no loop_tick_at in metadata" do
        it "returns :healthy (older process during a rolling deploy)" do
          result = data_source.send(:derive_process_status, false, { "pid" => 1 }, "consumer")
          expect(result).to eq(:healthy)
        end
      end

      context "when consumer stall_threshold is nil" do
        before { Pgbus.configuration.stall_threshold = nil }

        it "returns :healthy even with old loop_tick_at" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 999) }
          result = data_source.send(:derive_process_status, false, metadata, "consumer")
          expect(result).to eq(:healthy)
        end
      end

      context "when process is an unknown kind" do
        it "returns :healthy regardless of metadata" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 999) }
          result = data_source.send(:derive_process_status, false, metadata, "streamer")
          expect(result).to eq(:healthy)
        end
      end

      context "when dispatcher stall_threshold is nil" do
        before { Pgbus.configuration.stall_threshold = nil }

        it "returns :healthy even with old loop_tick_at" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 999) }
          result = data_source.send(:derive_process_status, false, metadata, "dispatcher")
          expect(result).to eq(:healthy)
        end
      end

      context "when worker has no loop_tick_at in metadata" do
        it "returns :healthy" do
          result = data_source.send(:derive_process_status, false, { "queues" => ["default"] }, "worker")
          expect(result).to eq(:healthy)
        end
      end

      context "when stall_threshold is nil" do
        before { Pgbus.configuration.stall_threshold = nil }

        it "returns :healthy even with old loop_tick_at" do
          metadata = { "loop_tick_at" => (Time.now.to_f - 999) }
          result = data_source.send(:derive_process_status, false, metadata, "worker")
          expect(result).to eq(:healthy)
        end
      end

      context "when metadata is not a Hash" do
        it "returns :healthy" do
          result = data_source.send(:derive_process_status, false, nil, "worker")
          expect(result).to eq(:healthy)
        end
      end
    end

    describe "#format_process (private)" do
      it "includes :status key in the result" do
        row = {
          "id" => "1",
          "kind" => "worker",
          "hostname" => "host1",
          "pid" => "123",
          "metadata" => { "loop_tick_at" => Time.now.to_f }.to_json,
          "last_heartbeat_at" => Time.now.to_s,
          "created_at" => Time.now.to_s
        }

        result = data_source.send(:format_process, row)
        expect(result).to have_key(:status)
        expect(result[:status]).to eq(:healthy)
        expect(result[:healthy]).to be true
      end

      it "marks stalled worker with status :stalled" do
        row = {
          "id" => "1",
          "kind" => "worker",
          "hostname" => "host1",
          "pid" => "123",
          "metadata" => { "loop_tick_at" => (Time.now.to_f - 120) }.to_json,
          "last_heartbeat_at" => Time.now.to_s,
          "created_at" => Time.now.to_s
        }

        result = data_source.send(:format_process, row)
        expect(result[:status]).to eq(:stalled)
        expect(result[:healthy]).to be false
      end
    end
  end
end
