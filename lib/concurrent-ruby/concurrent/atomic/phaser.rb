require 'concurrent/errors'
require 'concurrent/synchronization/lockable_object'
require 'concurrent/utility/native_integer'

module Concurrent

  # A reusable synchronization barrier, similar in spirit to {CyclicBarrier}
  # and {CountDownLatch}, but supporting a dynamically changing number of
  # parties.
  #
  # Unlike a `CyclicBarrier`, which is constructed for a fixed number of
  # parties, a `Phaser` lets parties {#register} (or {#bulk_register}) and
  # {#arrive_and_deregister} at any time, which makes it suitable for
  # pipelines in which the number of tasks varies from one iteration to the
  # next. Synchronization happens in phases: whenever the number of arrived
  # parties reaches the number of registered parties, the phase number
  # increases and the waiting parties are released. Each arrival counts
  # toward exactly one phase.
  #
  # A phaser with zero registered parties cannot be arrived at: {#arrive},
  # {#arrive_and_await_advance} and {#arrive_and_deregister} raise
  # {Concurrent::IllegalOperationError} because every arrival has to come
  # from a registered party. {#await_advance} still returns immediately
  # for phases the phaser has already moved past.
  #
  # A phaser can be terminated with {#force_termination}. Termination wakes
  # up all waiting threads; synchronization methods stop blocking and
  # phase-returning methods return negative values from then on.
  #
  # @example
  #   phaser = Concurrent::Phaser.new
  #   phaser.register # the main thread participates as well
  #
  #   workers = 3.times.map do
  #     phaser.register
  #     Thread.new do
  #       # ... work for this round ...
  #       phaser.arrive_and_await_advance
  #       # ... final work, then drop out of the following rounds ...
  #       phaser.arrive_and_deregister
  #     end
  #   end
  #
  #   phaser.arrive_and_await_advance # release the workers
  #   phaser.arrive_and_await_advance # wait until all workers are done
  #   workers.each(&:join)
  class Phaser < Synchronization::LockableObject

    # Create a new `Phaser`.
    #
    # @param [Integer] parties the number of parties required to advance to
    #   the next phase; more parties may be registered later with
    #   {#register} and {#bulk_register}
    #
    # @raise [ArgumentError] if `parties` is not an integer or is negative
    def initialize(parties = 0)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive parties

      super(&nil)
      synchronize { ns_initialize parties }
    end

    # @return [Integer] the current phase number, or a negative value if
    #   the phaser is terminated
    def phase
      synchronize { ns_current_phase }
    end

    # @return [Integer] the number of parties currently registered
    def registered_parties
      synchronize { @parties }
    end

    # @return [Integer] the number of registered parties that have already
    #   arrived at the current phase
    def arrived_parties
      synchronize { @arrived }
    end

    # @return [Integer] the number of registered parties that have not yet
    #   arrived at the current phase
    def unarrived_parties
      synchronize { @parties - @arrived }
    end

    # @return [Boolean] true if the phaser has been terminated
    def terminated?
      synchronize { @terminated }
    end

    # Adds a new party. The registration is atomic with respect to phase
    # advancement: even if the phase advances concurrently with this call,
    # the new party is counted toward exactly one phase, the one whose
    # number is returned.
    #
    # @return [Integer] the phase the party was registered in, or a
    #   negative value if the phaser is terminated
    def register
      synchronize do
        return ns_current_phase if @terminated
        @parties += 1
        @phase
      end
    end

    # Adds multiple new parties at once.
    #
    # @param [Integer] parties the number of parties to register
    #
    # @return [Integer] the phase the parties were registered in, or a
    #   negative value if the phaser is terminated
    #
    # @raise [ArgumentError] if `parties` is not an integer or is negative
    def bulk_register(parties)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive parties

      synchronize do
        return ns_current_phase if @terminated
        @parties += parties
        @phase
      end
    end

    # Arrives at the current phase without waiting for the other parties.
    #
    # @return [Integer] the phase arrived at, or a negative value if the
    #   phaser is terminated
    #
    # @raise [Concurrent::IllegalOperationError] if the number of arrivals
    #   would exceed the number of registered parties
    def arrive
      synchronize do
        return ns_current_phase if @terminated
        ns_arrive
      end
    end

    # Arrives at the current phase and waits until all registered parties
    # have arrived and the phase has advanced.
    #
    # @return [Integer] the phase arrived at, or a negative value if the
    #   phaser is terminated
    #
    # @raise [Concurrent::IllegalOperationError] if the number of arrivals
    #   would exceed the number of registered parties
    def arrive_and_await_advance
      synchronize do
        return ns_current_phase if @terminated

        phase = ns_arrive
        ns_wait_until { @terminated || @phase != phase }
        @terminated ? ns_current_phase : phase
      end
    end

    # Arrives at the current phase and deregisters, reducing the number of
    # parties required to advance the current and all future phases by one.
    #
    # @return [Integer] the phase arrived at, or a negative value if the
    #   phaser is terminated
    #
    # @raise [Concurrent::IllegalOperationError] if there are no registered
    #   parties
    def arrive_and_deregister
      synchronize do
        return ns_current_phase if @terminated
        if @parties == 0
          raise IllegalOperationError, 'cannot deregister, there are no registered parties'
        end

        phase    = @phase
        @parties -= 1
        ns_advance if @arrived >= @parties
        phase
      end
    end

    # Waits until the phase advances from the given phase. Returns
    # immediately if the phaser has already moved past `phase` or if it is
    # terminated.
    #
    # @param [Integer] phase the phase to wait on
    #
    # @return [Integer] the current phase, or a negative value if the
    #   phaser is terminated
    def await_advance(phase)
      synchronize do
        ns_wait_until { @terminated || @phase != phase }
        ns_current_phase
      end
    end

    # Waits until the phase advances from the given phase, giving up after
    # `timeout` seconds. A timeout only ends this wait: it never changes
    # the arrival counts or the phase, so the other parties are not
    # affected. Callers can detect a timeout by comparing the return value
    # with `phase`.
    #
    # @param [Integer] phase the phase to wait on
    # @param [Numeric, nil] timeout the number of seconds to wait, or `nil`
    #   to wait indefinitely
    #
    # @return [Integer] the current phase (equal to `phase` on timeout), or
    #   a negative value if the phaser is terminated
    def await_advance_interruptibly(phase, timeout = nil)
      synchronize do
        ns_wait_until(timeout) { @terminated || @phase != phase }
        ns_current_phase
      end
    end

    # Terminates the phaser. All waiting threads are released, waiting
    # methods stop blocking and phase-returning methods return negative
    # values. Termination cannot be undone.
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

    # @!visibility private
    def ns_initialize(parties)
      @phase      = 0
      @parties    = parties
      @arrived    = 0
      @terminated = false
    end

    # @!visibility private
    # The current phase, negative when terminated so waiters released by
    # termination can be told apart from normally released ones.
    def ns_current_phase
      @terminated ? -(@phase + 1) : @phase
    end

    # @!visibility private
    # Records one arrival, advancing the phase when it completes.
    # @return [Integer] the phase arrived at
    def ns_arrive
      if @arrived >= @parties
        raise IllegalOperationError,
              "arrived parties (#{@arrived + 1}) would exceed registered parties (#{@parties})"
      end

      phase    = @phase
      @arrived += 1
      ns_advance if @arrived == @parties
      phase
    end

    # @!visibility private
    # Advances to the next phase, releasing all waiting parties. The
    # arrival count is reset so no arrival is ever counted toward two
    # phases.
    def ns_advance
      @phase   += 1
      @arrived  = 0
      ns_broadcast
    end
  end
end
