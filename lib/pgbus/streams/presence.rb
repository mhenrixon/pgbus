# frozen_string_literal: true

module Pgbus
  module Streams
    # Presence tracking for SSE streams. A "presence member" is an
    # arbitrary identifier (typically a user id) marked as currently
    # subscribed to a given stream. Members live in pgbus_presence_members
    # with a (stream_name, member_id) primary key and a `last_seen_at`
    # timestamp; the table is the source of truth across all Puma workers
    # and Falcon reactors.
    #
    # Typical usage in a controller:
    #
    #   def show
    #     @room = Room.find(params[:id])
    #     Pgbus.stream(@room).presence.join(
    #       member_id: current_user.id.to_s,
    #       metadata: { name: current_user.name, avatar: current_user.avatar_url }
    #     )
    #   end
    #
    # And when the user leaves:
    #
    #   Pgbus.stream(@room).presence.leave(member_id: current_user.id.to_s)
    #
    # Reading the current member list:
    #
    #   Pgbus.stream(@room).presence.members
    #     # => [{ "id" => "7", "metadata" => {"name" => ..., "avatar" => ...},
    #     #      "joined_at" => "...", "last_seen_at" => "..." }]
    #
    # The join/leave operations also fire a stream broadcast (via the
    # caller-provided block) so connected clients see the change in real
    # time. The library emits the broadcast through the regular pgbus
    # stream pipeline, which means it's transactional, replayable, and
    # filterable just like any other broadcast.
    #
    # NOT included in this v1:
    #   - Automatic connection-driven join/leave (the application must
    #     call join/leave explicitly).
    #   - Automatic stale-member sweeping (call Presence.sweep!(stream)
    #     from a cron or after a heartbeat to expire idle members).
    #   - Built-in DOM events on the <pgbus-stream-source> element
    #     (the application's broadcast block decides the HTML).
    class Presence
      def initialize(stream)
        @stream = stream
      end

      # Adds (or refreshes) a member on this stream. Idempotent: calling
      # join twice with the same member_id updates last_seen_at and
      # metadata without creating a duplicate row. Yields the member
      # hash to the optional block so the caller can render an
      # `<turbo-stream action="append">` to broadcast the join.
      def join(member_id:, metadata: {})
        member_id = member_id.to_s
        record = upsert_member(member_id, metadata)

        if block_given?
          html = yield(record)
          @stream.broadcast(html) if html.is_a?(String) && !html.empty?
        end

        record
      end

      # Removes a member. Yields the (former) member hash to the
      # optional block so the caller can broadcast a `remove` action.
      # Returns the deleted record, or nil if the member wasn't present.
      def leave(member_id:)
        member_id = member_id.to_s
        record = delete_member(member_id)
        return nil unless record

        if block_given?
          html = yield(record)
          @stream.broadcast(html) if html.is_a?(String) && !html.empty?
        end

        record
      end

      # Refreshes the last_seen_at timestamp without re-broadcasting.
      # Used as a heartbeat mechanism: clients ping touch periodically
      # to stay in the member list, and a sweeper expires anyone who
      # hasn't pinged in N seconds.
      def touch(member_id:)
        member_id = member_id.to_s
        connection.exec_params(<<~SQL, [@stream.name, member_id])
          UPDATE pgbus_presence_members
          SET last_seen_at = NOW()
          WHERE stream_name = $1 AND member_id = $2
        SQL
      end

      # Returns the current list of members on this stream as an
      # array of hashes (id, metadata, joined_at, last_seen_at).
      def members
        rows = connection.exec_params(<<~SQL, [@stream.name])
          SELECT member_id, metadata, joined_at, last_seen_at
          FROM pgbus_presence_members
          WHERE stream_name = $1
          ORDER BY joined_at
        SQL
        rows.to_a.map do |row|
          {
            "id" => row["member_id"],
            "metadata" => parse_metadata(row["metadata"]),
            "joined_at" => row["joined_at"],
            "last_seen_at" => row["last_seen_at"]
          }
        end
      end

      # Returns the count of current members. Faster than members.size
      # because it doesn't deserialize metadata.
      def count
        rows = connection.exec_params(<<~SQL, [@stream.name])
          SELECT COUNT(*) AS n FROM pgbus_presence_members WHERE stream_name = $1
        SQL
        rows.first["n"].to_i
      end

      # Removes members whose last_seen_at is older than the given cutoff.
      # Returns the number of expired rows. Use as a sweeper:
      #
      #   Pgbus.stream("room:42").presence.sweep!(older_than: 60.seconds.ago)
      #
      # Atomically claims the deletion via DELETE RETURNING so multiple
      # workers running the sweep concurrently won't double-emit leave
      # events.
      def sweep!(older_than:)
        rows = connection.exec_params(<<~SQL, [@stream.name, older_than])
          DELETE FROM pgbus_presence_members
          WHERE stream_name = $1 AND last_seen_at < $2
          RETURNING member_id, metadata, joined_at, last_seen_at
        SQL
        rows.to_a.size
      end

      private

      def upsert_member(member_id, metadata)
        rows = connection.exec_params(<<~SQL, [@stream.name, member_id, JSON.generate(metadata)])
          INSERT INTO pgbus_presence_members (stream_name, member_id, metadata, joined_at, last_seen_at)
          VALUES ($1, $2, $3::jsonb, NOW(), NOW())
          ON CONFLICT (stream_name, member_id)
          DO UPDATE SET metadata = EXCLUDED.metadata, last_seen_at = NOW()
          RETURNING member_id, metadata, joined_at, last_seen_at
        SQL
        row = rows.first
        {
          "id" => row["member_id"],
          "metadata" => parse_metadata(row["metadata"]),
          "joined_at" => row["joined_at"],
          "last_seen_at" => row["last_seen_at"]
        }
      end

      def delete_member(member_id)
        rows = connection.exec_params(<<~SQL, [@stream.name, member_id])
          DELETE FROM pgbus_presence_members
          WHERE stream_name = $1 AND member_id = $2
          RETURNING member_id, metadata, joined_at, last_seen_at
        SQL
        row = rows.first
        return nil unless row

        {
          "id" => row["member_id"],
          "metadata" => parse_metadata(row["metadata"]),
          "joined_at" => row["joined_at"],
          "last_seen_at" => row["last_seen_at"]
        }
      end

      def parse_metadata(value)
        return value if value.is_a?(Hash)
        return {} if value.nil? || value.empty?

        JSON.parse(value)
      rescue JSON::ParserError
        {}
      end

      def connection
        # Respect multi-database setups (connects_to). When
        # Pgbus.configuration.connects_to is set, the pgbus tables
        # live in a separate database accessed via Pgbus::BusRecord;
        # the default path uses ActiveRecord::Base. Matches the
        # canonical pattern in Pgbus::Process::QueueLock#connection
        # and Pgbus::Configuration's connection probe.
        unless defined?(::ActiveRecord::Base)
          raise Pgbus::ConfigurationError,
                "Pgbus::Streams::Presence requires ActiveRecord (no AR connection available)"
        end

        if Pgbus.configuration.connects_to
          Pgbus::BusRecord.connection.raw_connection
        else
          ::ActiveRecord::Base.connection.raw_connection
        end
      end
    end
  end
end
