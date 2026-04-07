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
      end
    end
  end
end
