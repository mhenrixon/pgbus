# frozen_string_literal: true

module Pgbus
  module ExecutionPools
    class AsyncPool
      attr_reader :capacity

      IDLE_WAIT_INTERVAL = 0.01

      def initialize(capacity:, on_state_change: nil)
        @capacity = capacity
        @on_state_change = on_state_change
        @available_capacity = capacity
        @mutex = Mutex.new
        @state_mutex = Mutex.new
        @shutdown_flag = false
        @fatal_error = nil
        @boot_queue = Thread::Queue.new
        @pending = Thread::Queue.new

        validate_dependencies!
        @reactor_thread = start_reactor
        result = @boot_queue.pop
        raise result if result.is_a?(Exception)
      end

      def post(&block)
        raise_if_fatal!
        raise "Execution pool is shutting down" if shutdown?

        reserved = false
        reserve_capacity!
        reserved = true
        @pending << block
      rescue StandardError
        restore_capacity if reserved
        raise
      end

      def available_capacity
        raise_if_fatal!
        @mutex.synchronize { @available_capacity }
      end

      def idle?
        available_capacity.positive?
      end

      def shutdown
        @state_mutex.synchronize do
          return false if @shutdown_flag

          @shutdown_flag = true
        end
      end

      def shutdown?
        @state_mutex.synchronize { @shutdown_flag }
      end

      def wait_for_termination(timeout)
        @reactor_thread&.join(timeout)
      end

      def kill
        shutdown
        @reactor_thread&.kill
      end

      def metadata
        inflight = @mutex.synchronize { @available_capacity }
        {
          mode: :async,
          capacity: @capacity,
          busy: @capacity - inflight
        }
      end

      private

      def validate_dependencies!
        require "async"
        require "async/semaphore"
      rescue LoadError => e
        raise LoadError,
              "Async execution mode requires the `async` gem. " \
              "Add `gem \"async\"` to your Gemfile. Original error: #{e.message}"
      end

      # rubocop:disable Lint/RescueException
      def start_reactor
        Thread.new do
          Thread.current.name = "pgbus-async-reactor-#{object_id}"
          Async do |task|
            semaphore = Async::Semaphore.new(@capacity, parent: task)
            @boot_queue << :ready

            wait_for_executions(semaphore)
            wait_for_inflight
          end
        rescue Exception => e
          register_fatal_error(e)
          raise
        end
      end
      # rubocop:enable Lint/RescueException

      def wait_for_executions(semaphore)
        loop do
          schedule_pending(semaphore)
          break if shutdown? && @pending.empty?

          sleep(IDLE_WAIT_INTERVAL) if @pending.empty?
        end
      end

      def schedule_pending(semaphore)
        while (block = next_pending)
          semaphore.async do
            perform(block)
          end
        end
      end

      def next_pending
        @pending.pop(true)
      rescue ThreadError
        nil
      end

      # Supervisor-level rescue: catch any Exception raised from the user
      # block so capacity is always restored and the failure is logged.
      # The `async` gem uses Async::Stop / Async::Cancel (Exception subclasses,
      # NOT StandardError) to cancel tasks, and prior to issue #126 those
      # would leak past `rescue StandardError` and silently vanish.
      # Process-fatal signals still propagate so the supervisor can react.
      FATAL_EXCEPTIONS = [SystemExit, Interrupt, SignalException, NoMemoryError, SystemStackError].freeze
      private_constant :FATAL_EXCEPTIONS

      def perform(block)
        block.call
      rescue *FATAL_EXCEPTIONS
        raise
      rescue Exception => e # rubocop:disable Lint/RescueException
        Pgbus.logger.error { "[Pgbus] Async pool fiber error: #{e.class}: #{e.message}" }
      ensure
        restore_capacity
      end

      def reserve_capacity!
        @mutex.synchronize do
          raise "Execution pool is at capacity" if @available_capacity <= 0

          @available_capacity -= 1
        end
      end

      def restore_capacity
        should_notify = @mutex.synchronize do
          @available_capacity += 1
          @available_capacity.positive?
        end
        @on_state_change&.call if should_notify
      end

      def register_fatal_error(error)
        @state_mutex.synchronize { @fatal_error ||= error }
        @boot_queue << error if @boot_queue.empty?
        @on_state_change&.call
      end

      def raise_if_fatal!
        error = @state_mutex.synchronize { @fatal_error }
        raise error if error
      end

      def wait_for_inflight
        sleep(IDLE_WAIT_INTERVAL) while inflight?
      end

      def inflight?
        @mutex.synchronize { @available_capacity < @capacity }
      end
    end
  end
end
