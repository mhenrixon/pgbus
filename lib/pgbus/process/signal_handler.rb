# frozen_string_literal: true

module Pgbus
  module Process
    module SignalHandler
      def self.included(base)
        base.attr_reader :signal_queue
      end

      def setup_signals
        @signal_queue = Queue.new
        @previous_handlers = {}

        %w[INT TERM QUIT].each do |sig|
          @previous_handlers[sig] = trap(sig) { @signal_queue << sig }
        end
      end

      def restore_signals
        @previous_handlers&.each do |sig, handler|
          trap(sig, handler || "DEFAULT")
        end
      end

      def process_signals
        while (sig = @signal_queue.pop(true) rescue nil)
          case sig
          when "INT", "TERM"
            graceful_shutdown
          when "QUIT"
            immediate_shutdown
          end
        end
      end

      def graceful_shutdown
        raise NotImplementedError
      end

      def immediate_shutdown
        raise NotImplementedError
      end
    end
  end
end
