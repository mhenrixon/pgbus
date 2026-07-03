# frozen_string_literal: true

module Pgbus
  module Process
    module MemoryUsage
      MEMORY_CHECK_TTL = 5

      @mutex = Mutex.new
      @cached_value = nil
      @cached_at = nil

      class << self
        def current_mb
          @mutex.synchronize do
            now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

            return @cached_value if @cached_value && @cached_at && (now - @cached_at) < MEMORY_CHECK_TTL

            @cached_value = read_memory
            @cached_at = now
            @cached_value
          end
        end

        def reset!
          @mutex.synchronize do
            @cached_value = nil
            @cached_at = nil
          end
        end

        private

        def read_memory
          if RUBY_PLATFORM.include?("darwin")
            `ps -o rss= -p #{::Process.pid}`.to_i / 1024
          else
            begin
              File.read("/proc/#{::Process.pid}/statm").split[1].to_i * 4096 / (1024 * 1024)
            rescue Errno::ENOENT
              0
            end
          end
        end
      end
    end
  end
end
