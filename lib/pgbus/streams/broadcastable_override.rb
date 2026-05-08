# frozen_string_literal: true

module Pgbus
  module Streams
    # Runtime patch for `Turbo::Broadcastable` that adds `durable:` kwarg
    # support to all broadcast helpers. Applied at Rails engine boot time
    # when `defined?(::Turbo::Broadcastable)` — see `Pgbus::Engine`.
    #
    # Instance methods extract `durable:` from kwargs and set a thread-local
    # (`Thread.current[:pgbus_broadcast_durable]`) that
    # `TurboBroadcastable#broadcast_stream_to` reads. The thread-local is
    # always cleaned up after the broadcast, even on error.
    #
    # Class methods (`broadcasts_to`, `broadcasts_refreshes_to`) accept
    # `durable:` and store it so the generated callbacks set the thread-local
    # before each broadcast.
    module BroadcastableOverride
      BROADCAST_METHODS = %i[
        broadcast_replace_to
        broadcast_append_to
        broadcast_prepend_to
        broadcast_update_to
        broadcast_remove_to
        broadcast_refresh_to
        broadcast_replace_later_to
        broadcast_append_later_to
        broadcast_prepend_later_to
        broadcast_update_later_to
        broadcast_refresh_later_to
      ].freeze

      BROADCAST_ACTION_METHODS = %i[
        broadcast_action_to
        broadcast_action_later_to
      ].freeze

      BROADCAST_METHODS.each do |method_name|
        define_method(method_name) do |*streamables, **kwargs|
          durable = kwargs.delete(:durable)
          with_pgbus_durable(durable) { super(*streamables, **kwargs) }
        end
      end

      BROADCAST_ACTION_METHODS.each do |method_name|
        define_method(method_name) do |*streamables, action:, **kwargs|
          durable = kwargs.delete(:durable)
          with_pgbus_durable(durable) { super(*streamables, action: action, **kwargs) }
        end
      end

      module ClassMethods
        def broadcasts_to(stream, durable: nil, inserts_by: :append, target: broadcast_target_default, **rendering)
          if durable.nil?
            after_create_commit lambda {
              broadcast_action_later_to(
                stream.try(:call, self) || send(stream),
                action: inserts_by,
                target: target.try(:call, self) || target,
                **rendering
              )
            }
            after_update_commit -> { broadcast_replace_later_to(stream.try(:call, self) || send(stream), **rendering) }
            after_destroy_commit -> { broadcast_remove_to(stream.try(:call, self) || send(stream)) }
          else
            @pgbus_durable_streams ||= {}
            @pgbus_durable_streams[stream] = durable

            after_create_commit lambda {
              broadcast_action_later_to(
                stream.try(:call, self) || send(stream),
                action: inserts_by,
                target: target.try(:call, self) || target,
                durable: durable,
                **rendering
              )
            }
            after_update_commit lambda {
              broadcast_replace_later_to(stream.try(:call, self) || send(stream), durable: durable, **rendering)
            }
            after_destroy_commit lambda {
              broadcast_remove_to(stream.try(:call, self) || send(stream), durable: durable)
            }
          end
        end

        def broadcasts_refreshes_to(stream, durable: nil)
          if durable.nil?
            after_commit -> { broadcast_refresh_later_to(stream.try(:call, self) || send(stream)) }
          else
            @pgbus_durable_streams ||= {}
            @pgbus_durable_streams[stream] = durable

            after_commit lambda {
              broadcast_refresh_later_to(stream.try(:call, self) || send(stream), durable: durable)
            }
          end
        end

        def pgbus_durable_streams
          @pgbus_durable_streams || {}
        end
      end

      def self.install!(mod)
        return if mod.ancestors.include?(self)

        mod.prepend(self)
        mod.singleton_class.prepend(ClassMethods) if mod.is_a?(Module) && !mod.is_a?(Class)
        mod.extend(ClassMethods) if mod.is_a?(Class)
      end

      private

      def with_pgbus_durable(value)
        return yield if value.nil?

        previous = Thread.current[:pgbus_broadcast_durable]
        Thread.current[:pgbus_broadcast_durable] = value
        yield
      ensure
        Thread.current[:pgbus_broadcast_durable] = previous unless value.nil?
      end
    end
  end
end
