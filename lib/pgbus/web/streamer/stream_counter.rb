# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Web
    module Streamer
      class StreamCounter
        def initialize
          @streams = Concurrent::Map.new
        end

        def increment_broadcasts(stream_name)
          counters_for(stream_name)[:broadcasts].increment
        end

        def increment_connections(stream_name)
          counters_for(stream_name)[:active_connections].increment
        end

        def decrement_connections(stream_name)
          counters_for(stream_name)[:active_connections].decrement
        end

        def increment_total_connections(stream_name)
          counters_for(stream_name)[:total_connections].increment
        end

        def broadcasts(stream_name)
          entry = @streams[stream_name]
          entry ? entry[:broadcasts].value : 0
        end

        def active_connections(stream_name)
          entry = @streams[stream_name]
          return 0 unless entry

          [entry[:active_connections].value, 0].max
        end

        def total_connections(stream_name)
          entry = @streams[stream_name]
          entry ? entry[:total_connections].value : 0
        end

        def snapshot
          result = {}
          @streams.each_pair do |name, counters|
            result[name] = {
              broadcasts: counters[:broadcasts].value,
              active_connections: [counters[:active_connections].value, 0].max,
              total_connections: counters[:total_connections].value
            }
          end
          result
        end

        def totals
          total_broadcasts = 0
          total_active = 0
          total_conns = 0
          stream_count = 0

          @streams.each_pair do |_name, counters|
            total_broadcasts += counters[:broadcasts].value
            total_active += [counters[:active_connections].value, 0].max
            total_conns += counters[:total_connections].value
            stream_count += 1
          end

          {
            broadcasts: total_broadcasts,
            active_connections: total_active,
            total_connections: total_conns,
            streams: stream_count
          }
        end

        private

        def counters_for(stream_name)
          @streams.compute_if_absent(stream_name) do
            {
              broadcasts: Concurrent::AtomicFixnum.new(0),
              active_connections: Concurrent::AtomicFixnum.new(0),
              total_connections: Concurrent::AtomicFixnum.new(0)
            }
          end
        end
      end
    end
  end
end
