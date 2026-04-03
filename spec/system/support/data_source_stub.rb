# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      attr_accessor :stats, :queues, :processes_list, :failed_events_list,
                    :dlq_messages_list, :events_list, :subscribers_list, :jobs_list,
                    :paused_queues
      attr_reader :calls

      def initialize
        @stats = default_stats
        @queues = default_queues
        @processes_list = default_processes
        @failed_events_list = []
        @dlq_messages_list = []
        @events_list = []
        @subscribers_list = []
        @jobs_list = []
        @paused_queues = []
        @calls = Hash.new { |h, k| h[k] = [] }
      end

      def summary_stats = @stats
      def queues_with_metrics = @queues
      def queue_detail(name) = @queues.find { |q| q[:name].include?(name) }
      def processes = @processes_list
      def failed_events(page: 1, per_page: 25) = @failed_events_list
      def failed_events_count = @failed_events_list.size
      def failed_event(id) = @failed_events_list.find { |e| e["id"].to_s == id.to_s }
      def dlq_messages(page: 1, per_page: 25) = @dlq_messages_list
      def dlq_message_detail(msg_id) = @dlq_messages_list.find { |m| m[:msg_id].to_s == msg_id.to_s }
      def processed_events(page: 1, per_page: 25) = @events_list
      def processed_events_count = @events_list.size
      def processed_event(id) = @events_list.find { |e| e["id"].to_s == id.to_s }
      def registered_subscribers = @subscribers_list
      def jobs(queue_name: nil, page: 1, per_page: 25) = @jobs_list
      def queue_paused?(name) = @paused_queues.include?(name)

      def purge_queue(name)          = record(:purge_queue, name)
      def drop_queue(name)           = record(:drop_queue, name)
      def pause_queue(name, reason: nil) = record(:pause_queue, name, reason)
      def resume_queue(name) = record(:resume_queue, name)
      def retry_job(queue_name, msg_id)   = record(:retry_job, queue_name, msg_id)
      def discard_job(queue_name, msg_id) = record(:discard_job, queue_name, msg_id)
      def retry_failed_event(id)     = record(:retry_failed_event, id)
      def discard_failed_event(id)   = record(:discard_failed_event, id)
      def retry_all_failed           = record(:retry_all_failed) && @failed_events_list.size
      def discard_all_failed         = record(:discard_all_failed) && @failed_events_list.size
      def discard_all_enqueued       = record(:discard_all_enqueued) && @jobs_list.size
      def retry_dlq_message(queue_name, msg_id) = record(:retry_dlq_message, queue_name, msg_id)
      def discard_dlq_message(queue_name, msg_id) = record(:discard_dlq_message, queue_name, msg_id)
      def retry_all_dlq              = record(:retry_all_dlq) && @dlq_messages_list.size
      def discard_all_dlq            = record(:discard_all_dlq) && @dlq_messages_list.size
      def replay_event(event)        = record(:replay_event, event)

      def called?(method_name) = @calls.key?(method_name)

      private

      def record(method_name, *args)
        @calls[method_name] << args
        true
      end

      def default_stats
        { total_queues: 2, total_depth: 15, total_visible: 12,
          active_processes: 1, failed_count: 3, dlq_depth: 2 }
      end

      def default_queues
        [
          { name: "pgbus_default", queue_length: 10, queue_visible_length: 8,
            oldest_msg_age_sec: 120, newest_msg_age_sec: 5, total_messages: 500 },
          { name: "pgbus_default_dlq", queue_length: 2, queue_visible_length: 2,
            oldest_msg_age_sec: 3600, newest_msg_age_sec: 1800, total_messages: 5 }
        ]
      end

      def default_processes
        [{ id: 1, kind: "worker", hostname: "test-host", pid: 12_345,
           metadata: { "queues" => "default" }, last_heartbeat_at: Time.now, healthy: true,
           created_at: Time.now }]
      end
    end
  end
end

RSpec.configure do |config|
  config.before(:each, type: :system) do
    @stub_data_source = Pgbus::Test::StubDataSource.new

    Pgbus.configure do |c|
      c.web_live_updates = false
      c.web_refresh_interval = 0
      c.web_data_source = @stub_data_source
    end
  end

  config.after(:each, type: :system) do
    Pgbus.configure do |c|
      c.web_data_source = nil
    end
  end
end
