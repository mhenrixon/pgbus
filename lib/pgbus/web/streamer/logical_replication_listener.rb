# frozen_string_literal: true

require "stringio"
require "socket"

module Pgbus
  module Web
    module Streamer
      # An alternative to `Listener` that wakes the dispatcher via Postgres
      # logical replication (WAL streaming) instead of LISTEN/NOTIFY.
      #
      # Why this exists:
      #
      #   LISTEN/NOTIFY does not survive PgBouncer's transaction-pool mode.
      #   Hosted Postgres products like PlanetScale only ship transaction-mode
      #   pooling, which silently drops LISTEN at every COMMIT boundary —
      #   notifications go to /dev/null and SSE wake-ups never fire.
      #
      #   Logical replication uses a separate replication-protocol connection
      #   (`replication=database`). PgBouncer treats those as pass-through even
      #   in transaction mode, so the wake-up signal arrives whether the rest
      #   of the app talks to the pooler or the direct port.
      #
      # API parity with Listener:
      #
      #   - Same constructor signature (pg_connection: is repurposed as the
      #     replication-protocol PG::Connection; the caller wires it up).
      #   - Same `start` / `stop` / `ensure_listening` / `remove_listening`
      #     contract so Instance and StreamEventDispatcher need no changes.
      #   - Same `WakeMessage(queue_name:, payload:)` posted to dispatch_queue.
      #
      # Semantic difference:
      #
      #   - The WAL stream surfaces every INSERT on every `pgmq.q_pgbus_*`
      #     table, not just queues that the streamer is "listening" to. We
      #     keep a per-queue interest set in `@listening_to` (queue names,
      #     not channel names — different from Listener's CHANNEL_PREFIX
      #     scheme) and drop WAL events for queues no one cares about.
      #
      # Permissions:
      #
      #   The PG role used by the streamer needs REPLICATION attribute, plus
      #   CONNECT on the database. The publication and replication slot are
      #   created on first `start` if missing — owner of the slot must match
      #   the connecting role.
      #
      # Operational notes:
      #
      #   - One replication slot per streamer process. Each Puma worker that
      #     starts a Streamer::Instance creates its own slot named
      #     `pgbus_streamer_<host>_<pid>`.
      #   - On graceful shutdown the slot is dropped. On crash it lingers
      #     and retains WAL until cleaned up (see Streamer::Instance for the
      #     orphan-sweep hook).
      #   - Slot names are bounded to PG's 63-byte identifier limit.
      class LogicalReplicationListener
        WakeMessage = Listener::WakeMessage

        # PGMQ queue tables live in the `pgmq` schema and are prefixed
        # `q_pgbus_`. Stripping the prefix yields the pgbus queue name
        # used by Pgbus.stream(name).broadcast(content).
        QUEUE_TABLE_PREFIX = "q_pgbus_"
        QUEUE_SCHEMA       = "pgmq"

        PUBLICATION_NAME = "pgbus_stream_inserts"
        SLOT_PLUGIN      = "pgoutput"

        # pgoutput protocol message types (single-byte tags at start of payload).
        # https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
        MSG_BEGIN    = "B"
        MSG_COMMIT   = "C"
        MSG_RELATION = "R"
        MSG_INSERT   = "I"

        attr_reader :listening_to

        def initialize(pg_connection:, dispatch_queue:, health_check_ms:, logger: Pgbus.logger, slot_name: nil)
          @conn = pg_connection
          @dispatch_queue = dispatch_queue
          @health_check_ms = health_check_ms # unused; replication uses its own keepalive cadence
          @logger = logger
          @slot_name = slot_name || default_slot_name
          @listening_to = Set.new # queue names with at least one subscriber
          @relations = {}         # relation_oid → queue_name (filled from RELATION messages)
          @running = false
          @thread = nil
          @last_lsn = 0
          @commands_mutex = Mutex.new
        end

        def start
          return if @running

          ensure_publication!
          ensure_slot!

          @running = true
          @thread = Thread.new { run_loop }
          self
        end

        def stop
          return unless @running

          @running = false
          begin
            @conn.close if @conn.respond_to?(:close)
          rescue StandardError => e
            @logger.debug { "[Pgbus::Streamer::LogicalReplicationListener] connection close failed (best effort): #{e.message}" }
          end
          @thread&.join(5)
          @thread = nil
          drop_slot_quietly
          self
        end

        # The WAL stream is always-on once the slot is created. ensure_listening
        # here just records that the dispatcher cares about this queue, so we
        # only emit WakeMessage for matching INSERTs.
        def ensure_listening(queue_name)
          @commands_mutex.synchronize { @listening_to.add(queue_name) }
        end

        def remove_listening(queue_name)
          @commands_mutex.synchronize { @listening_to.delete(queue_name) }
        end

        private

        # PG allows up to 63 chars in an identifier. Encode host+pid into
        # something stable per process but unique across hosts. If the
        # composed name is too long we hash it.
        def default_slot_name
          base = "pgbus_streamer_#{Socket.gethostname}_#{::Process.pid}".gsub(/[^a-zA-Z0-9_]/, "_")
          return base if base.length <= 63

          require "digest"
          "pgbus_streamer_#{Digest::SHA1.hexdigest(base)[0, 20]}_#{::Process.pid}"
        end

        def ensure_publication!
          # Idempotent: CREATE PUBLICATION fails if it exists. We check first
          # because there's no portable IF NOT EXISTS for publications across
          # PG 14 and 15+ ([the syntax exists from PG 15]). pgmq adds queue
          # tables at runtime, so we want a FOR TABLES IN SCHEMA publication
          # rather than enumerating tables.
          with_admin_connection do |admin|
            exists = admin.exec_params(
              "SELECT 1 FROM pg_publication WHERE pubname = $1",
              [PUBLICATION_NAME]
            ).any?
            next if exists

            # PG 15+ syntax — pgmq runs on PG 14+ but FOR TABLES IN SCHEMA
            # is PG 15+. For PG 14 environments the operator must create a
            # FOR ALL TABLES publication manually; we surface a clear error.
            begin
              admin.exec("CREATE PUBLICATION #{PUBLICATION_NAME} FOR TABLES IN SCHEMA #{QUEUE_SCHEMA} WITH (publish = 'insert')")
            rescue PG::DuplicateObject
              # Another worker won the race between our SELECT and CREATE.
              nil
            rescue PG::SyntaxError => e
              raise Pgbus::ConfigurationError,
                    "Cannot CREATE PUBLICATION FOR TABLES IN SCHEMA (#{e.message}). " \
                    "PG 14 or older — create the publication manually with " \
                    "FOR ALL TABLES or enumerate pgmq.q_pgbus_* tables explicitly."
            end
          end
        end

        def ensure_slot!
          with_admin_connection do |admin|
            exists = admin.exec_params(
              "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
              [@slot_name]
            ).any?
            next if exists

            begin
              admin.exec_params(
                "SELECT pg_create_logical_replication_slot($1, $2)",
                [@slot_name, SLOT_PLUGIN]
              )
            rescue PG::DuplicateObject
              # Another worker created the slot between our SELECT and create.
              nil
            end
          end
        end

        # Drops this listener's slot during graceful shutdown so it doesn't
        # linger and pin WAL. On crash this won't run — the orphan-sweep
        # hook in Streamer::Instance is the safety net.
        def drop_slot_quietly
          with_admin_connection do |admin|
            admin.exec_params(
              "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
              [@slot_name]
            )
          end
        rescue PG::Error => e
          @logger.warn { "[Pgbus::Streamer::LogicalReplicationListener] drop slot failed: #{e.message}" }
        end

        # The replication-protocol connection (@conn) can't run arbitrary SQL —
        # only the special replication commands. For publication/slot
        # bootstrap we need a regular connection. Use a short-lived one
        # built from the same connection_options as the main streamer conn.
        def with_admin_connection
          require "pg" unless defined?(::PG::Connection)
          opts = Pgbus.configuration.connection_options
          admin = case opts
                  when String then ::PG.connect(opts)
                  when Hash   then ::PG.connect(**opts)
                  else
                    raise Pgbus::ConfigurationError,
                          "Cannot build admin connection for LogicalReplicationListener from #{opts.class}"
                  end
          yield admin
        ensure
          admin&.close
        end

        def run_loop
          @conn.exec("START_REPLICATION SLOT #{@slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{PUBLICATION_NAME}')")
          poll_interval = (@health_check_ms / 1000.0 / 5.0).clamp(0.01, 0.1)
          loop do
            break unless @running

            # get_copy_data(true) returns:
            #   String — a CopyData frame
            #   false  — no data available right now (libpq's async read path)
            #   nil    — end of COPY (server closed the stream)
            data = @conn.get_copy_data(true)
            case data
            when nil
              break # server ended the stream
            when false
              @conn.consume_input
              sleep poll_interval
            else
              handle_copy_message(data)
            end
          end
        rescue IOError, PG::Error => e
          # #stop closes the connection to interrupt get_copy_data
          @logger.warn { "[Pgbus::Streamer::LogicalReplicationListener] #{e.class}: #{e.message}" } if @running
        end

        # Replication protocol wraps WAL data in CopyData frames. The first
        # byte is the protocol message type ('w' = XLogData with payload,
        # 'k' = primary keepalive). For XLogData, bytes 1..8 are the start
        # LSN, 9..16 are the end LSN, 17..24 are the server send time, and
        # the rest is the pgoutput message.
        def handle_copy_message(data)
          case data[0]
          when "w"
            # XLogData header: 'w' Int64(start_lsn) Int64(end_lsn) Int64(send_time)
            # Track the end LSN so standby status replies confirm progress and
            # the slot can release WAL we've already processed.
            @last_lsn = data[9, 8].unpack1("Q>")
            payload = data[25..]
            handle_pgoutput_message(payload)
          when "k"
            # Keepalive: 'k' Int64(end_lsn) Int64(server_time) Int8(reply_now).
            # When the server asks for a reply, send one with the latest LSN
            # we observed in XLogData so the slot advances.
            reply_now = data[17].unpack1("C").nonzero?
            send_standby_status if reply_now
          end
        end

        def handle_pgoutput_message(payload)
          tag = payload[0]
          case tag
          when MSG_RELATION then handle_relation(payload)
          when MSG_INSERT   then handle_insert(payload)
          when MSG_BEGIN, MSG_COMMIT
            # No-op for the spike. A full impl would track LSN here.
          end
        end

        # RELATION message format (pgoutput proto v1):
        #   Byte('R') Int32(oid) String(schema) String(table) Int8(replica_identity)
        #   Int16(natts) [Int8(flags) String(name) Int32(type_oid) Int32(type_mod)] x natts
        # We only need oid, schema, table.
        def handle_relation(payload)
          io = StringIO.new(payload)
          io.read(1) # 'R'
          oid = io.read(4).unpack1("N")
          schema = read_cstring(io)
          table = read_cstring(io)
          return unless schema == QUEUE_SCHEMA && table.start_with?(QUEUE_TABLE_PREFIX)

          queue_name = table[QUEUE_TABLE_PREFIX.length..]
          @relations[oid] = queue_name
        end

        # INSERT message format (pgoutput proto v1):
        #   Byte('I') Int32(relation_oid) Byte('N') TupleData
        # For the spike we only need relation_oid to look up the queue.
        def handle_insert(payload)
          oid = payload[1, 4].unpack1("N")
          queue_name = @relations[oid]
          return unless queue_name

          @commands_mutex.synchronize do
            return unless @listening_to.include?(queue_name)
          end

          @dispatch_queue << WakeMessage.new(queue_name: queue_name)
        end

        def read_cstring(io)
          buf = +""
          while (ch = io.read(1)) && ch != "\x00"
            buf << ch
          end
          buf
        end

        # Send a standby status update to keep the replication slot from
        # holding WAL forever. In the spike we just reply with the LSN we
        # received last; production would track this more carefully.
        def send_standby_status
          # 'r' = Standby status update: receive_lsn, flush_lsn, apply_lsn, clock_ms, reply_now
          msg = ["r", @last_lsn, @last_lsn, @last_lsn, (Time.now.to_f * 1_000_000).to_i, 0].pack("aQ>Q>Q>Q>C")
          @conn.put_copy_data(msg)
        rescue PG::Error => e
          @logger.warn { "[Pgbus::Streamer::LogicalReplicationListener] standby reply failed: #{e.message}" }
        end
      end
    end
  end
end
