# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Worker-local in-memory registry of SSE connections indexed by stream
      # name and connection id. Thread-safe via a single mutex. Reads return
      # snapshots so iterators never hold the lock.
      #
      # Registry operations are O(1) under contention (mutex-protected hash
      # lookups), and iteration is O(n) over a snapshot. The data structure is
      # deliberately boring — the interesting parts of the streamer (LISTEN
      # multiplexing, write scheduling, replay race handling) live elsewhere.
      class Registry
        def initialize
          @mutex = Mutex.new
          @by_stream = {}
          @by_id = {}
        end

        def register(connection)
          @mutex.synchronize do
            existing = @by_id[connection.id]
            return if existing.equal?(connection)

            # Registration of a new object under an existing id is a
            # defensive path — SecureRandom.hex(8) collisions are
            # astronomical, but the Registry's invariant is "@by_stream
            # only contains objects that are also in @by_id", so we
            # must scrub the old entry from its stream index before
            # overwriting. Otherwise connections_for(stream) would
            # return orphaned objects and writes would go nowhere.
            evict_from_stream(existing) if existing

            @by_id[connection.id] = connection
            (@by_stream[connection.stream_name] ||= Set.new).add(connection)
          end
        end

        def unregister(connection)
          @mutex.synchronize do
            existing = @by_id.delete(connection.id)
            return unless existing

            set = @by_stream[existing.stream_name]
            next unless set

            set.delete(existing)
            @by_stream.delete(existing.stream_name) if set.empty?
          end
        end

        def lookup(id)
          @mutex.synchronize { @by_id[id] }
        end

        # Returns a snapshot Array of connections on the given stream.
        # Mutating the result has no effect on the registry.
        def connections_for(stream_name)
          @mutex.synchronize do
            set = @by_stream[stream_name]
            set ? set.to_a : []
          end
        end

        # Snapshot list of stream names with at least one registered connection.
        def streams
          @mutex.synchronize { @by_stream.keys.dup }
        end

        def empty?(stream_name)
          @mutex.synchronize do
            set = @by_stream[stream_name]
            set.nil? || set.empty?
          end
        end

        def size
          @mutex.synchronize { @by_id.size }
        end

        # Yields every registered connection across all streams. The iteration
        # walks a snapshot so the block may safely call back into register/
        # unregister without risk of deadlock or skipped items.
        def each_connection(&)
          snapshot = @mutex.synchronize { @by_id.values.dup }
          snapshot.each(&)
        end

        private

        # Must be called while holding @mutex.
        def evict_from_stream(connection)
          set = @by_stream[connection.stream_name]
          return unless set

          set.delete(connection)
          @by_stream.delete(connection.stream_name) if set.empty?
        end
      end
    end
  end
end
