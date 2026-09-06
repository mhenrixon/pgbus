# frozen_string_literal: true

module Pgbus
  class Client
    # Retries Postgres deadlocks / lock timeouts around stream-queue
    # notify-insert setup.
    #
    # 0.16.1 already skips `pgmq.enable_notify_insert` when the notify
    # trigger is current (#403 treats a concurrent CREATE as success), but
    # two processes can still both see "not current" and deadlock — or hit
    # lock_timeout / statement_timeout-while-locking — on `DROP TRIGGER`
    # under AccessExclusiveLock. PGMQ wraps those as
    # `PGMQ::Errors::ConnectionError`, which `with_stale_connection_retry`
    # must not treat as stale (that helper is idle-socket / no-SQL-sent
    # only).
    #
    # Sleep here — in the rescue, *outside* the yielded block — so the
    # backoff never runs while `synchronized` holds `@pgmq_mutex`. Callers
    # must wrap only the notify-setup path, not hold the mutex across this
    # method. Missing-queue and permission errors stay unretried.
    module NotifyLockRetry
      ATTEMPTS = 3
      DELAYS = [0.05, 0.15, 0.35].freeze

      # Also match statement-timeout lock waits: pooled connections often
      # keep lock_timeout above statement_timeout, so a DROP TRIGGER waiter
      # dies as PG::QueryCanceled ("canceling statement due to statement
      # timeout / while locking"), not LockWaitTimeout. Bare statement
      # timeout (no lock context) is deterministic — do not retry.
      LOCK_FAILURE_PATTERN = /
        deadlock\ detected
        |lock\ not\ available
        |canceling\ statement\ due\ to\ lock\ timeout
      /ix
      STATEMENT_TIMEOUT = /canceling statement due to statement timeout/i
      LOCK_WAIT_CONTEXT = /while locking/i

      def self.retryable?(error)
        current = error
        depth = 0
        while current && depth < 8
          return true if lock_class?(current) || lock_message?(current)

          current = current.cause
          depth += 1
        end
        false
      end

      def self.lock_class?(error)
        return false if error.nil?

        (defined?(::PG::TRDeadlockDetected) && error.is_a?(::PG::TRDeadlockDetected)) ||
          (defined?(::PG::LockNotAvailable) && error.is_a?(::PG::LockNotAvailable)) ||
          (defined?(::ActiveRecord::Deadlocked) && error.is_a?(::ActiveRecord::Deadlocked)) ||
          (defined?(::ActiveRecord::LockWaitTimeout) && error.is_a?(::ActiveRecord::LockWaitTimeout))
      end
      private_class_method :lock_class?

      def self.lock_message?(error)
        return false if error.nil?

        message = error.message.to_s
        return true if LOCK_FAILURE_PATTERN.match?(message)

        STATEMENT_TIMEOUT.match?(message) && LOCK_WAIT_CONTEXT.match?(message)
      end
      private_class_method :lock_message?

      private

      def with_notify_lock_retry(stream_name)
        attempts = 0
        begin
          yield
        rescue StandardError => e
          attempts += 1
          raise unless attempts < ATTEMPTS && NotifyLockRetry.retryable?(e)

          # Sleep here — in the rescue, *outside* the yielded block — so the
          # backoff never runs while @pgmq_mutex is held: on the shared-
          # connection path the mutex lives inside `synchronized` within the
          # yielded block, and the raise unwinds out of it (releasing the
          # mutex) before we get here. See DELAYS. Clamp the index to the
          # last delay so a future ATTEMPTS > DELAYS.size never sleeps nil.
          sleep DELAYS[[attempts - 1, DELAYS.size - 1].min]

          Pgbus.logger.warn do
            "[Pgbus::Client] Retrying stream-queue notify setup after a Postgres lock failure " \
              "(attempt #{attempts}/#{ATTEMPTS} stream=#{stream_name}): #{e.message}"
          end
          retry
        end
      end
    end
  end
end
