# frozen_string_literal: true

require "fugit"

module Pgbus
  module Recurring
    class Task
      attr_reader :key, :class_name, :command, :schedule, :queue_name,
                  :arguments, :priority, :description

      def self.from_configuration(key, **options)
        options = options.transform_keys(&:to_sym)
        new(
          key: key,
          class_name: options[:class],
          command: options[:command],
          schedule: options[:schedule],
          queue_name: options[:queue],
          arguments: Array(options[:args]),
          priority: options.fetch(:priority, 0).to_i,
          description: options[:description]
        )
      end

      def initialize(key:, class_name: nil, command: nil, schedule: nil,
                     queue_name: nil, arguments: [], priority: 0, description: nil)
        @key = key
        @class_name = class_name
        @command = command
        @schedule = schedule
        @queue_name = queue_name
        @arguments = arguments || []
        @priority = priority || 0
        @description = description
        @errors = []
      end

      def valid?
        @errors = []
        validate!
        @errors.empty?
      end

      def errors
        valid? unless defined?(@validated)
        @errors
      end

      def parsed_schedule
        @parsed_schedule ||= parse_schedule
      end

      def next_time(from = Time.now)
        parsed_schedule&.next_time(from)&.to_t
      end

      def previous_time(from = Time.now)
        parsed_schedule&.previous_time(from)&.to_t
      end

      def human_schedule
        return nil unless parsed_schedule

        parsed_schedule.to_cron_s
      end

      def job_class
        class_name&.safe_constantize
      end

      def to_h
        {
          key: key,
          class_name: class_name,
          command: command,
          schedule: schedule,
          queue_name: queue_name,
          arguments: arguments,
          priority: priority,
          description: description
        }
      end

      private

      def validate!
        @validated = true

        if schedule.nil? || schedule.to_s.strip.empty?
          @errors << "Schedule is required"
          return
        end

        @errors << "Either class or command is required" if class_name.nil? && command.nil?

        return if parsed_schedule.is_a?(Fugit::Cron)

        @errors << "Schedule '#{schedule}' is not a valid cron expression"
      end

      def parse_schedule
        return nil if schedule.nil? || schedule.to_s.strip.empty?

        parsed = Fugit.parse(schedule.to_s, multi: :fail)
        parsed.is_a?(Fugit::Cron) ? parsed : nil
      rescue ArgumentError
        nil
      end
    end
  end
end
