require 'concurrent/errors'
require 'concurrent/utility/native_integer'
require 'concurrent/utility/monotonic_time'

module Concurrent

  # A reusable synchronization barrier with a dynamically changing number of
  # registered parties. A phaser can be arranged in a tree. A child phaser
  # counts as one party of its parent, regardless of how many parties are
  # registered with the child.
  #
  # Unlike {CyclicBarrier}, waiting after an arrival does not consume another
  # arrival. Registrations made while a phaser is completing a phase are blocked
  # until the phase advance finishes, and are then counted in the new phase.
  #
  # @example Changing party count between phases
  #   phaser = Concurrent::Phaser.new(1)
  #   2.times { phaser.register }
  #   3.times { Thread.new { phaser.arrive_and_await_advance } }
  #   phaser.arrive_and_deregister
  class Phaser

    attr_reader :parent

    # Create a new phaser.
    #
    # @param [Phaser, Integer] parent_or_parties a parent phaser, or the
    #   number of parties for a root phaser
    # @param [Integer] parties the number of parties when a parent is supplied
    # @yield optional action executed by the thread completing a phase. It
    #   receives the completed phase and registered party count. Returning
    #   `true` terminates this phaser.
    # @raise [ArgumentError] when arguments or party counts are invalid
    # @raise [Concurrent::IllegalOperationError] when the parent has terminated
    def initialize(parent_or_parties = 0, parties = nil, &block)
      if parties.nil? && !parent_or_parties.nil? && parent_or_parties.is_a?(self.class)
        parent  = parent_or_parties
        parties = 0
      elsif parties.nil?
        parent  = nil
        parties = parent_or_parties
      else
        parent = parent_or_parties
        unless parent.nil? || parent.is_a?(self.class)
          raise ArgumentError, 'first argument must be a Concurrent::Phaser'
        end
      end

      parties ||= 0
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive parties

      @parent    = parent
      @root      = parent ? parent.root : self
      @children  = []
      @phase     = 0
      @registered = 0
      @unarrived  = 0
      @waiting_count = 0
      @register_waiting_count = 0
      @awaiting_advance = false
      @terminated = false
      @action    = block

      @mutex     = ::Mutex.new if root?
      @condition = ::ConditionVariable.new if root?

      if parent
        root_synchronize do
          begin
            raise IllegalOperationError, 'cannot register with a terminated phaser' if ns_terminated?(parent)

            @phase = parent.instance_variable_get(:@phase)
            parent.send(:ns_add_child, self)
            parent.send(:ns_wait_for_registration_generation)
            raise IllegalOperationError, 'cannot register with a terminated phaser' if ns_terminated?(parent)

            @phase = parent.instance_variable_get(:@phase)
            ns_register(parties)
            parent.send(:ns_register, 1)
          rescue StandardError
            parent.send(:ns_remove_child, self)
            raise
          end
        end
      else
        @mutex.synchronize { ns_register(parties) }
      end
    end

    # @return [Phaser] the root phaser of this phaser tree
    def root
      @root
    end

    # @return [Boolean] true when this phaser is the root of its tree
    def root?
      @root.equal?(self)
    end

    # Register one or more parties for the current phase.
    #
    # If all parties of this phaser have already arrived but the tree has not
    # finished advancing, this call blocks and joins the next phase instead.
    #
    # @param [Integer] parties number of parties to register
    # @return [Integer] the phase in which the registration was accepted
    # @raise [ArgumentError] when `parties` is not a non-negative integer
    # @raise [Concurrent::IllegalOperationError] when terminated
    def register(parties = 1)
      Utility::NativeInteger.ensure_integer_and_bounds parties
      Utility::NativeInteger.ensure_positive_and_no_zero parties

      root_synchronize do
        raise IllegalOperationError, 'cannot register with a terminated phaser' if @terminated

        @register_waiting_count += 1
        begin
          ns_wait_for_registration_generation
        ensure
          @register_waiting_count -= 1
        end
        raise IllegalOperationError, 'cannot register with a terminated phaser' if @terminated

        ns_register(parties)
        @phase
      end
    end
    alias_method :bulk_register, :register

    # Arrive at the current phase without waiting for the phase to advance.
    # Arrivals are not tied to the calling thread. Calling this method more
    # than once consumes multiple registered arrivals; an additional arrival
    # after all current arrivals raises rather than advancing twice.
    # @return [Integer] the phase to which this arrival applied
    # @raise [Concurrent::IllegalOperationError] when terminated, when no party
    #   is registered, or when this phase has already received all arrivals
    def arrive
      root_synchronize do
        raise IllegalOperationError, 'cannot arrive at a terminated phaser' if @terminated
        raise IllegalOperationError, 'no parties are registered' if @registered.zero?
        raise IllegalOperationError, 'all registered parties have already arrived' if @unarrived.zero?

        arrival_phase = @phase
        @unarrived -= 1
        ns_complete if @unarrived.zero?
        arrival_phase
      end
    end

    # Arrive and remove one party from future phases.
    #
    # @return [Integer] the phase to which this arrival applied
    # @raise [Concurrent::IllegalOperationError] for an invalid arrival
    def arrive_and_deregister
      root_synchronize do
        raise IllegalOperationError, 'cannot arrive at a terminated phaser' if @terminated
        raise IllegalOperationError, 'no parties are registered' if @registered.zero?
        raise IllegalOperationError, 'all registered parties have already arrived' if @unarrived.zero?

        arrival_phase = @phase
        @unarrived -= 1
        @registered -= 1
        ns_complete if @unarrived.zero?
        arrival_phase
      end
    end

    # Wait for the supplied or current phase to advance.
    #
    # A stale phase returns immediately. A finite timeout returns `false` when
    # the phase has not advanced; termination always returns the negative phase
    # rather than `false`. Waiting does not count as an arrival and does not
    # deregister the waiting party.
    #
    # @param [Integer, Numeric, nil] phase_or_timeout phase number, or timeout
    #   when it is a non-integer numeric value
    # @param [Numeric, nil] timeout maximum wait in seconds
    # @return [Integer, Boolean, nil] the new phase, a negative terminated
    #   phase, or `false`/`nil` on timeout
    def await_advance(phase_or_timeout = nil, timeout = nil)
      if phase_or_timeout.nil?
        requested_phase = nil
      elsif phase_or_timeout.is_a?(Integer)
        requested_phase = phase_or_timeout
      elsif phase_or_timeout.is_a?(Numeric)
        requested_phase = nil
        timeout = phase_or_timeout
      else
        raise ArgumentError, 'phase must be an integer or timeout must be numeric'
      end

      Utility::NativeInteger.ensure_integer_and_bounds requested_phase unless requested_phase.nil?
      raise ArgumentError, 'timeout must be numeric' unless timeout.nil? || timeout.is_a?(Numeric)

      root_synchronize do
        current = @phase
        requested_phase ||= current

        raise ArgumentError, 'cannot wait for a future phase' if requested_phase > current
        return current if requested_phase < current
        return current if @terminated

        deadline = timeout ? Concurrent.monotonic_time + timeout : nil
        @waiting_count += 1
        begin
          while !@terminated && @phase == requested_phase
            remaining = deadline ? deadline - Concurrent.monotonic_time : nil
            return false if remaining && remaining <= 0

            root_condition.wait(root_mutex, remaining)
          end
          @phase
        ensure
          @waiting_count -= 1
        end
      end
    end

    # Arrive at the current phase and wait for it to advance.
    #
    # @param [Numeric, nil] timeout maximum wait in seconds
    # @return [Integer, Boolean, nil] the new phase, a negative terminated
    #   phase, or `false`/`nil` on timeout
    def arrive_and_await_advance(timeout = nil)
      arrival_phase = arrive
      await_advance(arrival_phase, timeout)
    end
    alias_method :arrive_and_wait, :arrive_and_await_advance

    # Terminate this phaser and all of its descendants. Terminating a child
    # does not remove its one registration from its parent; the child must still
    # arrive or deregister if the parent should advance.
    #
    # @return [Integer] the negative terminated phase
    def force_terminate
      root_synchronize do
        @parent.send(:ns_remove_child, self) if @parent
        ns_terminate_subtree(self)
        root_condition.broadcast
        @phase
      end
    end
    alias_method :force_termination, :force_terminate
    alias_method :terminate, :force_terminate

    # @return [Integer] current phase, or a negative value when terminated
    def phase
      root_synchronize { @phase }
    end

    # @return [Integer] number of parties registered in this phase and future
    #   phases
    def registered_parties
      root_synchronize { @registered }
    end
    alias_method :parties, :registered_parties
    alias_method :get_registered_parties, :registered_parties

    # @return [Integer] number of parties that have arrived in this phase
    def arrived_parties
      root_synchronize { @registered - @unarrived }
    end
    alias_method :get_arrived_parties, :arrived_parties

    # @return [Integer] number of parties that have not yet arrived
    def unarrived_parties
      root_synchronize { @unarrived }
    end
    alias_method :get_unarrived_parties, :unarrived_parties

    # @return [Integer] number of threads currently waiting for phase advance
    def number_waiting
      root_synchronize { @waiting_count }
    end
    alias_method :waiting_count, :number_waiting

    # @return [Boolean] true when this phaser has been terminated
    def terminated?
      root_synchronize { @terminated }
    end
    alias_method :is_terminated, :terminated?

    private

    def root_mutex
      root? ? @mutex : @root.instance_variable_get(:@mutex)
    end

    def root_condition
      root? ? @condition : @root.instance_variable_get(:@condition)
    end

    def root_synchronize(&block)
      root_mutex.synchronize(&block)
    end

    def ns_add_child(child)
      @children << child
    end

    def ns_remove_child(child)
      @children.delete(child)
    end

    def ns_terminated?(phaser)
      phaser.instance_variable_get(:@terminated)
    end

    def ns_register(parties)
      @registered += parties
      @unarrived  += parties
    end

    def ns_wait_for_registration_generation
      while !@terminated && @awaiting_advance
        root_condition.wait(root_mutex)
      end
    end

    def ns_complete
      completed_phase = @phase
      @awaiting_advance = true
      should_terminate = @registered.zero?

      begin
        should_terminate = @action ? @action.call(completed_phase, @registered) == true : @registered.zero?
      rescue StandardError
        ns_terminate_subtree(self)
        if @parent
          @parent.send(:ns_remove_child, self)
          @parent.send(:ns_child_arrived, true)
        end
        root_condition.broadcast
        raise
      end

      if should_terminate
        ns_terminate_subtree(self)
        if @parent
          @parent.send(:ns_remove_child, self)
          @parent.send(:ns_child_arrived, true)
        end
        root_condition.broadcast
      elsif root?
        ns_advance_tree
        root_condition.broadcast
      else
        @parent.send(:ns_child_arrived, false)
      end
    end

    def ns_child_arrived(deregister)
      @registered -= 1 if deregister
      @unarrived -= 1
      ns_complete if @unarrived.zero?
    end

    def ns_advance_tree(next_phase = @phase + 1, node = self)
      node.instance_variable_set(:@phase, next_phase)
      node.instance_variable_set(:@unarrived, node.instance_variable_get(:@registered))
      node.instance_variable_set(:@awaiting_advance, false)
      node.instance_variable_get(:@children).each do |child|
        next if child.instance_variable_get(:@terminated)

        ns_advance_tree(next_phase, child)
      end
    end

    def ns_terminate_subtree(node)
      terminated_phase = node.instance_variable_get(:@phase)
      terminated_phase = -(terminated_phase + 1) unless terminated_phase.negative?
      node.instance_variable_set(:@phase, terminated_phase)
      node.instance_variable_set(:@terminated, true)
      node.instance_variable_get(:@children).each { |child| ns_terminate_subtree(child) }
    end
  end
end
