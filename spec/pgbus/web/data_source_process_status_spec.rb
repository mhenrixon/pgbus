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
