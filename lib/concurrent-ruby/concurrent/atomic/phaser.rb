require 'concurrent/errors'
require 'concurrent/synchronization/lockable_object'
require 'concurrent/utility/native_integer'

module Concurrent

  # A reusable synchronization barrier, similar in spirit to {CyclicBarrier},
  # but supporting a dynamically changing number of parties and tiering
  # (tree-like structuring) of phasers.
  #
  # Unlike a {CyclicBarrier}, the number of parties is not fixed: parties may
  # {#register} (and deregister with {#arrive_and_deregister}) at any time,
  # which makes a Phaser suitable for orchestrating stages in which a
  # varying number of subtasks is forked and joined.
  #
  # A phaser advances through numbered generations called *phases*, starting
  # at phase 0. A phase is completed (advanced) when all currently
  # registered parties have {#arrive}d in it. Waiting for the next phase is
  # done with {#await_advance} (or the combined {#arrive_and_await_advance});
  # only arrival moves the phaser forward, waiting never changes the number
  # of parties, so waiters which time out, are killed or raise do not shrink
  # the barrier.
  #
  # Phasers may be tiered: a phaser constructed with a parent occupies
  # exactly one party on its parent for as long as it has at least one
  # registered party of its own. When its last party deregisters it
  # automatically arrives and deregisters on the parent, and when it later
  # grows from zero parties again it re-registers with the parent. This lets
  # a whole subgroup of workers count as a single participant of the
  # higher-level barrier.
  #
  # Arrival counts are tracked, not threads: a single thread may arrive on
  # behalf of several parties, and it is up to the caller not to arrive more
  # times than the number of registered parties. Arriving (or deregistering)
  # when no unarrived parties remain raises {IllegalOperationError}, as does
  # any registration or arrival on a terminated phaser.
  #
  # A phaser terminates when {#force_termination} is called or when
  # {#on_advance} returns true (the default implementation terminates the
  # phaser when its last registered party deregisters). Waiting on a
  # terminated phaser never blocks; registration and arrival raise
  # {IllegalOperationError}.
  #
  # @example a subgroup whose size changes every phase counts once on the parent
  #   parent = Concurrent::Phaser.new(1) # the main thread
  #   group  = Concurrent::Phaser.new(0, parent)
  #
  #   [2, 3, 1].each do |subtasks|        # different size every phase
  #     subtasks.times { group.register }
  #     subtasks.times.map do
  #       Thread.new { group.arrive_and_deregister }
  #     end.each(&:join)
  #     parent.arrive_and_await_advance   # released once the whole group drained
  #   end
  #
  #   # (override #on_advance to keep using a group after it drained to zero
  #   #  parties, since by default a phaser terminates when that happens)
  class Phaser < Synchronization::LockableObject

    # @return [Phaser, nil] the parent phaser this phaser is tiered under
    attr_reader :parent

    # Create a new Phaser.
    #
    # @param [Integer] parties the number of parties required to advance to
    #   the next phase; may be zero and changed later with {#register}
    # @param [Phaser, nil] parent an optional parent phaser; while this phaser
    #   has at least one registered party it occupies exactly one party on the
    #   parent
    #
    # @raise [ArgumentError] if parties is not an integer or is negative, or
    #   if parent is not a Phaser
    def initialize(parties = 0, parent = nil)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      raise ArgumentError, 'parties cannot be negative' if parties < 0
      unless parent.nil? || parent.is_a?(Phaser)
        raise ArgumentError, 'parent must be a Concurrent::Phaser or nil'
      end

      super()
      synchronize { ns_initialize parties, parent }
    end

    # @return [Integer] the current phase number, starting at 0
    def phase
      synchronize { @phase }
    end

    # @return [Integer] the number of parties currently registered
    def registered_parties
      synchronize { @parties }
    end

    # @return [Integer] the number of registered parties that have already
    #   arrived in the current phase
    def arrived_parties
      synchronize { @arrived }
    end

    # @return [Integer] the number of registered parties that have not yet
    #   arrived in the current phase
    def unarrived_parties
      synchronize { @parties - @arrived }
    end

    # @return [Boolean] true if this phaser has been terminated
    def terminated?
      synchronize { @terminated }
    end

    # Register a new party. The party is expected to arrive in the current
    # and all future phases until it deregisters.
    #
    # Registration is atomic with respect to phase advancement: it either
    # happens entirely before an in-flight advance completes (and the new
    # party is counted in the current phase) or entirely after it (and the
    # party is counted in the next phase). The returned phase number always
    # tells which generation the party joined.
    #
    # @return [Integer] the phase the new party is counted in
    # @raise [IllegalOperationError] if the phaser is terminated
    def register
      bulk_register 1
    end

    # Register several new parties at once.
    #
    # @param [Integer] count the number of parties to add
    # @return [Integer] the phase the new parties are counted in
    # @raise [ArgumentError] if count is not a positive integer
    # @raise [IllegalOperationError] if the phaser is terminated
    def bulk_register(count)
      Utility::NativeInteger.ensure_integer_and_bounds count
      Utility::NativeInteger.ensure_positive_and_no_zero count
      synchronize do
        ns_check_operational
        @parties += count
        ns_sync_with_parent
        @phase
      end
    end

    # Arrive at the current phase without waiting for the others. When this
    # arrival completes the phase, the phase is advanced and all waiters are
    # released.
    #
    # @return [Integer] the phase that was arrived at
    # @raise [IllegalOperationError] if the phaser is terminated or if all
    #   registered parties have already arrived
    def arrive
      synchronize do
        ns_check_operational
        ns_check_arrivable
        phase    = @phase
        @arrived += 1
        ns_advance_if_arrived
        phase
      end
    end

    # Deregister one party and arrive on its behalf, reducing the number of
    # parties required to advance by one. The phase advances only if every
    # other registered party has already arrived. When the last registered
    # party deregisters the phaser releases its slot on the parent (if any)
    # and, unless {#on_advance} is overridden, terminates.
    #
    # @return [Integer] the phase that was arrived at
    # @raise [IllegalOperationError] if the phaser is terminated or if all
    #   registered parties have already arrived
    def arrive_and_deregister
      synchronize do
        ns_check_operational
        if @parties <= 0
          raise IllegalOperationError, 'no registered party to deregister'
        end
        phase = @phase
        @parties -= 1
        ns_advance_if_arrived
        ns_sync_with_parent
        phase
      end
    end

    # Arrive at the current phase and wait for it to advance.
    #
    # @param [Numeric, nil] timeout the number of seconds to wait for the
    #   phase to advance or nil to block indefinitely
    # @return [Integer, nil] the new phase number, or nil on timeout; the
    #   arrival is not rolled back on timeout
    # @raise [IllegalOperationError] if the phaser is terminated or if all
    #   registered parties have already arrived
    def arrive_and_await_advance(timeout = nil)
      await_advance arrive, timeout
    end

    # Wait until the phaser leaves the given phase.
    #
    # Waiting never registers a party and never changes the arrival counts,
    # so a waiter that times out, is killed or raises does not shrink the
    # barrier. Passing a stale phase (one the phaser has already moved past)
    # or a phase the phaser has not reached yet returns immediately without
    # blocking; waiting on a terminated phaser also returns immediately.
    #
    # @param [Integer] phase the phase to wait for the end of
    # @param [Numeric, nil] timeout the number of seconds to wait or nil to
    #   block indefinitely
    # @return [Integer, nil] the current phase number, or nil on timeout
    def await_advance(phase, timeout = nil)
      synchronize do
        return @phase if @terminated || phase != @phase
        ns_wait_until(timeout) { @terminated || @phase != phase } ? @phase : nil
      end
    end

    # Force this phaser into the terminated state regardless of the arrival
    # counts. All current and future waiters are released immediately;
    # subsequent registration and arrival raise {IllegalOperationError}. The
    # registration counts and the slot occupied on the parent (if any) are
    # left untouched, and tiered children are not affected.
    #
    # @return [nil]
    def force_termination
      synchronize do
        @terminated = true
        ns_broadcast
        nil
      end
    end

    protected

    # Hook invoked (while holding the lock) by the thread that completes a
    # phase, just before the phase number is incremented. Returning true
    # terminates the phaser. The default implementation terminates the phaser
    # when its last registered party deregisters. Overrides must not call
    # methods of this phaser, the lock is already held.
    #
    # @param [Integer] phase the phase that is about to be completed
    # @param [Integer] registered_parties the number of parties registered
    #   for the next phase
    # @return [Boolean] true to terminate the phaser
    def on_advance(phase, registered_parties)
      registered_parties.zero?
    end

    private

    def ns_initialize(parties, parent)
      @phase                  = 0
      @parties                = parties
      @arrived                = 0
      @parent                 = parent
      @terminated             = false
      @registered_with_parent = false
      ns_sync_with_parent
    end

    def ns_check_operational
      if @terminated
        raise IllegalOperationError, 'the phaser is terminated'
      end
    end

    def ns_check_arrivable
      if @arrived >= @parties
        raise IllegalOperationError,
              'all registered parties have already arrived in this phase'
      end
    end

    def ns_advance_if_arrived
      return unless @arrived >= @parties
      terminate = on_advance @phase, @parties
      @phase   += 1
      @arrived  = 0
      @terminated = true if terminate
      ns_broadcast
    end

    # Keep the invariant that this phaser occupies exactly one party on its
    # parent if and only if it has at least one registered party of its own.
    def ns_sync_with_parent
      return unless @parent
      if @parties > 0 && !@registered_with_parent
        @parent.register
        @registered_with_parent = true
      elsif @parties == 0 && @registered_with_parent
        @parent.arrive_and_deregister
        @registered_with_parent = false
      end
    end
  end
end
