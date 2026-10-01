require 'concurrent/synchronization/lockable_object'
require 'concurrent/utility/native_integer'

module Concurrent

  # A reusable synchronization aid, similar to {CyclicBarrier}, but supporting
  # a variable number of parties. A phaser keeps a generation counter, the
  # _phase_, which is incremented every time all registered parties have
  # arrived. Unlike a cyclic barrier, parties may join a phaser at any time
  # ({#register}), arrive without waiting for the others ({#arrive}), arrive
  # and wait for the phase to advance ({#arrive_and_wait}), or deregister
  # themselves after arriving ({#arrive_and_deregister}) so that they do not
  # take part in subsequent phases.
  #
  # A phase advances exactly once per round: a single `arrive` is always
  # counted in the phase the party was registered for, never in two phases.
  #
  # @example processing a variable number of tasks in rounds
  #   phaser = Concurrent::Phaser.new(1)
  #
  #   workers = 3.times.map do |i|
  #     phaser.register
  #     Thread.new do
  #       loop do
  #         # do one round of work ...
  #         phaser.arrive_and_wait
  #       end
  #     end
  #   end
  #
  #   phaser.arrive_and_wait # the controller takes part as well
  class Phaser < Synchronization::LockableObject

    # Create a new `Phaser`.
    #
    # @param [Integer] parties the number of parties initially registered,
    #   may be zero
    # @yield [phase, registered_parties] an optional block invoked by the
    #   thread which completes a phase, before waiting parties are released.
    #   When the block returns a truthy value the phaser is terminated;
    #   otherwise the next phase begins. By default a phaser without a block
    #   terminates as soon as the number of registered parties drops to zero.
    # @yieldparam [Integer] phase the phase that just completed
    # @yieldparam [Integer] registered_parties the number of parties
    #   registered for the next phase
    #
    # @raise [ArgumentError] if `parties` is not an integer or is negative
    def initialize(parties = 0, &on_advance)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive parties

      super(&nil)
      synchronize { ns_initialize parties, &on_advance }
    end

    # @return [Integer] the current phase number. The phase starts at zero and
    #   is incremented every time all registered parties arrive. When the
    #   phaser is terminated the returned value is negative, namely
    #   `-(phase_before_termination + 1)`.
    def phase
      synchronize { @phase }
    end

    # @return [Integer] the number of parties registered for the current phase
    def registered_parties
      synchronize { @registered }
    end
    alias_method :parties, :registered_parties

    # @return [Integer] the number of registered parties which have not yet
    #   arrived in the current phase
    def unarrived_parties
      synchronize { @unarrived }
    end

    # @return [Boolean] `true` if the phaser has been terminated, either with
    #   {#force_termination} or by the `on_advance` block
    def terminated?
      synchronize { @phase < 0 }
    end

    # Registers one or more new parties.
    #
    # When the current phase is still in progress the new parties have to
    #   arrive in it as well. If the phase is already complete (for example on
    #   a phaser whose parties all deregistered), registration first advances
    #   to the next phase, so a registration always belongs to exactly one
    #   phase, never to two.
    #
    # @param [Integer] parties the number of parties to register
    # @return [Integer] the phase number to which the registration applies;
    #   a negative value means the phaser was already terminated and the
    #   registration had no effect
    # @raise [ArgumentError] if `parties` is not a positive integer
    def register(parties = 1)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive_and_no_zero parties

      synchronize do
        unless @phase < 0
          # A phaser with no registered parties has trivially completed its
          # current phase, unless it was never used (still pristine at phase
          # zero). Finish that phase first so the new parties belong to the
          # new one.
          ns_finish_empty_phase if @registered == 0 && @phase != 0

          if @phase >= 0
            @registered += parties
            @unarrived  += parties
          end
        end
        @phase
      end
    end

    # Records one arrival in the current phase without waiting for the other
    # parties. It is a usage error to call this method when there is no
    # unarrived party.
    #
    # @return [Integer] the phase number the arrival was recorded in; a
    #   negative value means the phaser was terminated and the arrival had no
    #   effect
    # @raise [IllegalOperationError] if no party is still expected to arrive
    #   in the current phase (for example on a phaser with zero registered
    #   parties, or when a party arrives twice in the same phase)
    def arrive
      synchronize do
        return @phase if @phase < 0
        raise IllegalOperationError, 'there is no unarrived party' if @unarrived == 0

        arrival_phase = @phase
        begin
          ns_record_arrival
        rescue Exception
          # leave the phaser usable if the on_advance block failed
          @unarrived += 1
          raise
        end
        arrival_phase
      end
    end

    # Records one arrival and, unless the caller completed the phase, blocks
    # until the phase advances.
    #
    # @param [Numeric, nil] timeout the number of seconds to wait for the
    #   other parties, or `nil` to wait indefinitely. A timeout only ends this
    #   wait; the arrival itself is not undone, so the other parties are not
    #   affected and can still advance the phase.
    # @return [Integer] the phase number on return: the next phase number
    #   when the phase advanced, the (unchanged) arrival phase on timeout, or
    #   a negative value if the phaser terminated
    def arrive_and_wait(timeout = nil)
      synchronize do
        return @phase if @phase < 0
        raise IllegalOperationError, 'there is no unarrived party' if @unarrived == 0

        arrival_phase = @phase
        begin
          ns_record_arrival
        rescue Exception
          # leave the phaser usable if the on_advance block failed
          @unarrived += 1
          raise
        end

        if @phase == arrival_phase
          ns_wait_until(timeout) { @phase != arrival_phase }
        end
        @phase
      end
    end

    # Records one arrival and deregisters the party from all future phases.
    # The current phase still needs the arrival, but the party is removed
    # before the next phase starts.
    #
    # @return [Integer] the phase number the arrival was recorded in; a
    #   negative value means the phaser was terminated and the deregistration
    #   had no effect
    # @raise [IllegalOperationError] if no party is still expected to arrive
    #   in the current phase
    def arrive_and_deregister
      synchronize do
        return @phase if @phase < 0
        raise IllegalOperationError, 'there is no unarrived party' if @unarrived == 0

        arrival_phase = @phase
        @unarrived -= 1
        @registered -= 1
        begin
          ns_complete_phase if @unarrived == 0
        rescue Exception
          # leave the phaser usable if the on_advance block failed
          @unarrived += 1
          @registered += 1
          raise
        end
        arrival_phase
      end
    end

    # Waits until the given phase advances, without recording an arrival.
    #
    # A phaser with zero registered parties is trivially complete, so waiting
    # on its phase returns immediately.
    #
    # @param [Integer, nil] phase_to_await the phase to wait for, defaults to
    #   the current phase. Waiting for an earlier phase returns immediately.
    # @param [Numeric, nil] timeout the number of seconds to wait, or `nil`
    #   to wait indefinitely. A timeout only ends this wait.
    # @return [Integer] the phase number on return. It equals
    #   `phase_to_await` on timeout, otherwise it is the phase the phaser
    #   advanced to (a negative value when the phaser terminated)
    def await_advance(phase_to_await = nil, timeout = nil)
      synchronize do
        phase_to_await = @phase if phase_to_await.nil?

        if @phase >= 0 && @phase == phase_to_await && @unarrived > 0
          ns_wait_until(timeout) { @phase != phase_to_await }
        end
        @phase
      end
    end

    # Forces the phaser to terminate. All parties waiting on a phase are
    # released immediately, the phase number becomes negative and subsequent
    # registration, arrival and waiting calls return immediately without
    # changing the state.
    #
    # @return [Integer] the negative termination phase number
    def force_termination
      synchronize do
        if @phase >= 0
          @phase = -@phase - 1
          ns_broadcast
        end
        @phase
      end
    end

    private

    def ns_initialize(parties, &on_advance)
      @phase      = 0
      @registered = parties
      @unarrived  = parties
      @on_advance = on_advance
    end

    # Records the caller's arrival, completing the current phase if it was the
    # last expected arrival.
    def ns_record_arrival
      @unarrived -= 1
      ns_complete_phase if @unarrived == 0
    end

    # Advances or terminates a phaser with zero unarrived parties, releasing
    # every waiting thread.
    def ns_complete_phase
      should_terminate =
        if @on_advance
          @on_advance.call(@phase, @registered)
        else
          @registered == 0
        end

      if should_terminate
        @phase = -@phase - 1
      else
        @phase     += 1
        @unarrived = @registered
      end
      ns_broadcast
    end

    # Completes the trivially-finished phase of a phaser whose registered
    # parties all left, before new parties are registered.
    def ns_finish_empty_phase
      ns_complete_phase if @unarrived == 0
    end
  end
end
