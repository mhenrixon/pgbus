# frozen_string_literal: true

require "zlib"

module Pgbus
  module Process
    # Manages PostgreSQL advisory locks for single-active-consumer mode.
    # Only one worker process can hold the lock for a given queue at a time.
    # Other workers skip the queue and process other queues instead.
    #
    # Uses pg_try_advisory_lock (non-blocking) so workers never wait —
    # they simply skip queues they can't lock and try again next cycle.
    #
    # Locks are session-level and automatically released when the connection
    # closes (including on crash), so no manual cleanup is needed.
    class QueueLock
      # Use a fixed namespace to avoid collision with application advisory locks.
      # CRC32 of "pgbus_queue_lock" = 0x5067_6275
      LOCK_NAMESPACE = 0x5067_6275

      def initialize
        @held_locks = Concurrent::Map.new
      end

      # Try to acquire an advisory lock for the given queue name.
      # Returns true if acquired (or already held), false if another process holds it.
      def try_lock(queue_name)
        return true if @held_locks[queue_name]

        lock_id = lock_id_for(queue_name)
        acquired = connection.select_value(
          "SELECT pg_try_advisory_lock(#{LOCK_NAMESPACE}, #{lock_id})"
        )

        if acquired
          @held_locks[queue_name] = lock_id
          true
        else
          false
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Advisory lock failed for #{queue_name}: #{e.message}" }
        false
      end

      # Release the advisory lock for a queue. Called during shutdown.
      def unlock(queue_name)
        lock_id = @held_locks.delete(queue_name)
        return unless lock_id

        connection.select_value(
          "SELECT pg_advisory_unlock(#{LOCK_NAMESPACE}, #{lock_id})"
        )
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Advisory unlock failed for #{queue_name}: #{e.message}" }
      end

      # Release all held locks.
      def unlock_all
        @held_locks.each_key { |q| unlock(q) }
      end

      def locked?(queue_name)
        @held_locks.key?(queue_name)
      end

      def held_queues
        @held_locks.keys
      end

      private

      def lock_id_for(queue_name)
        # Use a stable hash to convert queue name to a 32-bit integer
        Zlib.crc32(queue_name.to_s) & 0x7FFFFFFF
      end

      def connection
        if Pgbus.configuration.connects_to
          Pgbus::ApplicationRecord.connection
        else
          ActiveRecord::Base.connection
        end
      end
    end
  end
end
