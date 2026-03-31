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
        @self_pipe_r, @self_pipe_w = IO.pipe

        %w[INT TERM QUIT].each do |sig|
          @previous_handlers[sig] = trap(sig) do
            @signal_queue << sig
            # Wake any IO.select / interruptible_sleep call
            @self_pipe_w.write_nonblock(".", exception: false)
          end
        end
      end

      def restore_signals
        @previous_handlers&.each do |sig, handler|
          trap(sig, handler || "DEFAULT")
        end
        @self_pipe_r&.close unless @self_pipe_r&.closed?
        @self_pipe_w&.close unless @self_pipe_w&.closed?
      end

      def process_signals
        while (sig = begin
          @signal_queue.pop(true)
        rescue StandardError
          nil
        end)
          case sig
          when "INT", "TERM"
            graceful_shutdown
          when "QUIT"
            immediate_shutdown
          end
        end
      end

      # Sleep that can be interrupted by signals. Use this instead of Kernel#sleep
      # in any loop that needs to respond to INT/TERM/QUIT promptly.
      def interruptible_sleep(seconds)
        @self_pipe_r.wait_readable(seconds)
        drain_self_pipe
      end

      def graceful_shutdown
        raise NotImplementedError
      end

      def immediate_shutdown
        raise NotImplementedError
      end

      private

      def drain_self_pipe
        loop { @self_pipe_r.read_nonblock(256) }
      rescue IO::WaitReadable, EOFError
        # pipe drained
      end
    end
  end
end
