require 'concurrent/actor/behaviour/abstract'

module Concurrent
  module Actor
    module Behaviour

      # Handles supervised actors. Handle configures what to do with failed child: :terminate!, :resume!, :reset!,
      # or :restart!. Strategy sets :one_for_one (restarts just failed actor), :one_for_all (restarts all child
      # actors), or :rest_for_one (restarts the failed child and all children added after it).
      #
      # ## :rest_for_one
      #
      # Requires the :restart! handle. It expresses "later children depend on earlier children":
      # children are supervised in the order they were added to the supervisor ({Core#children}).
      # When a child fails, children added before it are left untouched, the failed child and its
      # successors are terminated in reverse order and then replaced by newly spawned actors in the
      # original registration order.
      #
      # Replaced children get new {Reference}s and the old references are terminated. The order of
      # {Core#children} is preserved, so callers can always re-fetch the current references from the
      # supervisor (a reference obtained before a restart has to be considered stale).
      #
      # Options (a Hash passed after the strategy, see {Behaviour.supervising}):
      #
      # -   :max_restarts (default 3) - max number of restarts allowed within :within_seconds.
      #     When exceeded, the supervisor pauses itself with {MaxRestartIntensityExceeded} so its own
      #     parent supervisor takes over, instead of restarting in an infinite loop.
      # -   :within_seconds (default 5) - the time window for :max_restarts.
      # -   :clock - an injectable clock, a callable returning monotonic time in seconds,
      #     defaults to Concurrent.monotonic_time.
      # -   :restart_policies - a Hash mapping child names to :permanent, :transient
      #     or :temporary. A :temporary child is never restarted and its failure triggers
      #     no restart at all; :transient and :permanent children are restarted when they
      #     fail. Successors terminated by a :rest_for_one restart are replaced as well,
      #     unless they are :temporary.
      # -   :default_policy (default :permanent) - policy of children not listed
      #     in :restart_policies.
      #
      # A child failure arriving while a restart is already in progress does not start
      # a second restart flow.
      #
      # @note TODO missing example
      # @note this will change in next version to support supervision trees better
      class Supervising < Abstract
        # internal message delivering a child respawn result back to the supervisor
        RESPAWNED = :__rest_for_one_respawned__

        DEFAULT_MAX_RESTARTS   = 3
        DEFAULT_WITHIN_SECONDS = 5

        def initialize(core, subsequent, core_options, handle, strategy, options = {})
          super core, subsequent, core_options
          @handle   = Match! handle, :terminate!, :resume!, :reset!, :restart!
          @strategy = case @handle
                      when :terminate!
                        Match! strategy, nil
                      when :resume!
                        Match! strategy, :one_for_one
                      when :reset!
                        Match! strategy, :one_for_one, :one_for_all
                      when :restart!
                        Match! strategy, :one_for_one, :one_for_all, :rest_for_one
                      end

          return unless @strategy == :rest_for_one

          options           = Type! options, Hash
          @max_restarts     = options.fetch :max_restarts, DEFAULT_MAX_RESTARTS
          @within_seconds   = options.fetch :within_seconds, DEFAULT_WITHIN_SECONDS
          @clock            = Type! options.fetch(:clock, -> { Concurrent.monotonic_time }), Proc
          @default_policy   = Match! options.fetch(:default_policy, :permanent),
                                     :permanent, :transient, :temporary
          policies          = Type! options.fetch(:restart_policies, {}), Hash
          @restart_policies = policies.each_with_object({}) do |(name, policy), all|
            all[name.to_sym] = Match!(policy, :permanent, :transient, :temporary)
          end

          @restart_timestamps = []
          @restarting         = nil
        end

        def on_envelope(envelope)
          message = envelope.message
          case message
          when Exception, :paused
            if @strategy == :rest_for_one
              rest_for_one_restart envelope.sender
            else
              receivers = if @strategy == :one_for_all
                            children
                          else
                            [envelope.sender]
                          end
              receivers.each { |ch| ch << @handle }
            end
          when ::Array
            if @strategy == :rest_for_one && rest_for_one_internal(envelope)
              MESSAGE_PROCESSED
            else
              pass envelope
            end
          else
            pass envelope
          end
        end

        private

        # A child failed, starts a :rest_for_one restart unless one is already running.
        def rest_for_one_restart(failed)
          # do not stack a second restart flow while one is in progress
          return MESSAGE_PROCESSED if @restarting
          # :temporary children are never restarted and trigger no restart of others
          return MESSAGE_PROCESSED if restart_policy(failed) == :temporary
          # give up and let the parent supervisor take over when restarting too often
          return MESSAGE_PROCESSED unless record_restart_attempt

          ordered = children
          index   = ordered.index failed
          targets = index ? ordered[index..-1] : [failed]
          # capture replacement specs before terminating, so each restarted child is
          # replaced by an equivalent actor with a new reference at its original position
          specs   = targets.reject { |child| restart_policy(child) == :temporary }.
              map { |child| respawn_options_of(child) }

          @restarting = { stopping: targets.reverse, starting: specs, current: nil, pending: nil }
          stop_next_child
          MESSAGE_PROCESSED
        end

        # Handles internal messages of an ongoing :rest_for_one restart.
        # @return [true, false] whether the envelope was consumed
        def rest_for_one_internal(envelope)
          command, *args = envelope.message
          case command
          when :terminated
            return false unless @restarting
            child_terminated envelope.sender
            true
          when RESPAWNED
            return false unless @restarting
            child_respawned(*args)
            true
          else
            false
          end
        end

        # Terminates the remaining children one by one in reverse order,
        # waiting for each termination before stopping the next one.
        def stop_next_child
          loop do
            child = @restarting[:stopping].shift
            if child.nil?
              @restarting[:current] = nil
              return start_next_child
            end
            next if terminated? child
            @restarting[:current] = child
            child << :terminate!
            return
          end
        end

        def child_terminated(sender)
          state = @restarting
          if state[:current] == sender
            state[:current] = nil
            stop_next_child
          else
            # terminated on its own before being stopped
            state[:stopping].delete sender
          end
        end

        # Spawns the replacements one by one in the original registration order,
        # waiting for each child to initialize before starting the next one.
        def start_next_child
          spec = @restarting[:starting].first
          if spec.nil?
            @restarting = nil
          else
            spawn_child spec
          end
          nil
        end

        def spawn_child(spec)
          @restarting[:pending] = spec
          initialized = Concurrent::Promises.resolvable_future
          options     = spec.merge initialized: initialized, parent: reference
          block       = options.delete :block
          Core.new options, &block
          initialized.on_fulfillment! { |new_reference| reference.tell [RESPAWNED, spec, true, new_reference] }
          initialized.on_rejection! { |reason| reference.tell [RESPAWNED, spec, false, reason] }
          nil
        rescue => error
          reference.tell [RESPAWNED, spec, false, error]
          nil
        end

        def child_respawned(spec, success, _value)
          state = @restarting
          return unless state && state[:pending].equal?(spec)
          state[:pending] = nil
          if success
            state[:starting].shift
            start_next_child
          elsif record_restart_attempt
            # a failed start counts towards the restart intensity, retry
            spawn_child spec
          end
          # else: restart intensity exceeded, the failure was escalated to the parent
          nil
        end

        # @return [Boolean] false when the max restart intensity was exceeded and
        #   the failure was escalated to the parent supervisor instead of restarting again
        def record_restart_attempt
          now = @clock.call
          @restart_timestamps.reject! { |timestamp| now - timestamp > @within_seconds }
          if @restart_timestamps.size >= @max_restarts
            escalate!
            false
          else
            @restart_timestamps << now
            true
          end
        end

        # Gives up restarting and lets the parent supervisor take over by pausing
        # this actor with an error, instead of restarting in an infinite loop.
        def escalate!
          @restarting = nil
          error       = MaxRestartIntensityExceeded.new reference, @max_restarts, @within_seconds
          pausing     = behaviour Pausing
          if pausing
            pausing.pause! error
          else
            terminate! error
          end
          nil
        end

        def restart_policy(child)
          @restart_policies.fetch child.name.to_sym, @default_policy
        end

        def respawn_options_of(child)
          child.send(:core).respawn_options
        end

        def terminated?(child)
          termination = child.send(:core).behaviour Termination
          termination.nil? || termination.terminated?
        end
      end
    end
  end
end
