require 'set'
require 'concurrent/errors'
require 'concurrent/synchronization/object'
require 'concurrent/utility/monotonic_time'

module Concurrent

  # A `TVar` is a transactional variable - a single-element container that
  # is used as part of a transaction - see `Concurrent::atomically`.
  #
  # @!macro thread_safe_variable_comparison
  #
  # {include:file:docs-source/tvar.md}
  class TVar < Synchronization::Object
    safe_initialization!

    # Create a new `TVar` with an initial value.
    def initialize(value)
      @value = value
      @lock = Mutex.new
      @waiters = []
    end

    # Get the value of a `TVar`.
    def value
      Concurrent::atomically do
        Transaction::current.read(self)
      end
    end

    # Set the value of a `TVar`.
    def value=(value)
      Concurrent::atomically do
        Transaction::current.write(self, value)
      end
    end

    # @!visibility private
    def unsafe_value # :nodoc:
      @value
    end

    # @!visibility private
    def unsafe_value=(value) # :nodoc:
      @value = value
    end

    # @!visibility private
    def unsafe_lock # :nodoc:
      @lock
    end

    # Register a transaction that is about to sleep until this `TVar`
    # changes. Must be called with `@lock` held so that no commit can slip
    # a write in between the caller's last read and the registration.
    #
    # @!visibility private
    def unsafe_add_waiter(waiter) # :nodoc:
      @waiters << waiter
    end

    # @!visibility private
    def unsafe_remove_waiter(waiter) # :nodoc:
      @waiters.delete(waiter)
    end

    # Wake every transaction sleeping on this `TVar`. Must be called with
    # `@lock` held, after the new value has been stored.
    #
    # @!visibility private
    def unsafe_signal_waiters # :nodoc:
      waiters, @waiters = @waiters, []
      waiters.each { |waiter| waiter.signal }
    end

  end

  # Run a block that reads and writes `TVar`s as a single atomic transaction.
  # With respect to the value of `TVar` objects, the transaction is atomic, in
  # that it either happens or it does not, consistent, in that the `TVar`
  # objects involved will never enter an illegal state, and isolated, in that
  # transactions never interfere with each other. You may recognise these
  # properties from database transactions.
  #
  # There are some very important and unusual semantics that you must be aware of:
  #
  # * Most importantly, the block that you pass to atomically may be executed
  #     more than once. In most cases your code should be free of
  #     side-effects, except for via TVar.
  #
  # * If an exception escapes an atomically block it will abort the transaction.
  #
  # * It is undefined behaviour to use callcc or Fiber with atomically.
  #
  # * If you create a new thread within an atomically, it will not be part of
  #     the transaction. Creating a thread counts as a side-effect.
  #
  # Transactions within transactions are flattened to a single transaction.
  #
  # A transaction may call `Concurrent::retry_transaction` when a condition
  # it depends on does not hold yet. Rather than busy-waiting, the
  # transaction then releases its locks and sleeps until one of the `TVar`s
  # it has read so far is modified by another committed transaction, at
  # which point the block is run again. `Concurrent::or_else` composes two
  # alternative transactional blocks, running the second when the first
  # retries.
  #
  # @param timeout [nil, Numeric] maximum number of seconds to spend sleeping
  #   inside `retry_transaction` across all attempts of the transaction.
  #   When the deadline passes `Concurrent::TimeoutError` is raised. Only
  #   meaningful on the outermost `atomically`; nested calls are flattened
  #   and ignore it.
  #
  # @example
  #   a = new TVar(100_000)
  #   b = new TVar(100)
  #
  #   Concurrent::atomically do
  #     a.value -= 10
  #     b.value += 10
  #   end
  def atomically(timeout: nil)
    raise ArgumentError.new('no block given') unless block_given?

    # Get the current transaction

    transaction = Transaction::current

    # Are we not already in a transaction (not nested)?

    if transaction.nil?
      # New transaction

      deadline = timeout && Concurrent.monotonic_time + timeout

      begin
        # Retry loop

        loop do

          # Create a new transaction

          transaction = Transaction.new
          Transaction::current = transaction

          # Run the block, aborting on exceptions

          begin
            result = yield
          rescue Transaction::AbortError
            transaction.abort
            result = Transaction::ABORTED
          rescue Transaction::LeaveError
            transaction.abort
            break result
          rescue Transaction::RetryError
            # The transaction asked to wait for its read set to change.
            # This releases the transaction's locks before sleeping, so a
            # committing transaction can never be missed, then re-runs the
            # block. Raises Concurrent::TimeoutError once the deadline has
            # passed.

            remaining = deadline && (deadline - Concurrent.monotonic_time)
            transaction.wait_for_change(remaining)
            next
          rescue Exception
            transaction.abort
            raise
          end
          # If we can commit, break out of the loop

          if result != Transaction::ABORTED
            if transaction.commit
              break result
            end
          end
        end
      ensure
        # Clear the current transaction

        Transaction::current = nil
      end
    else
      # Nested transaction - flatten it and just run the block

      yield
    end
  end

  # Abort a currently running transaction - see `Concurrent::atomically`.
  def abort_transaction
    raise Transaction::AbortError.new
  end

  # Leave a transaction without committing or aborting - see `Concurrent::atomically`.
  def leave_transaction
    raise Transaction::LeaveError.new
  end

  # Abort the current transaction and, instead of busy-waiting, put it to
  # sleep until another committed transaction modifies one of the `TVar`s
  # the current transaction has read so far. The whole `atomically` block
  # is then run again from the start. Use this when a condition the
  # transaction depends on does not hold yet.
  #
  # A transaction that has read nothing cannot be woken by anyone; calling
  # this with an empty read set raises
  # `Concurrent::Transaction::InvalidRetryError` unless the enclosing
  # `atomically` was given a `timeout`, in which case it simply sleeps out
  # the deadline and raises `Concurrent::TimeoutError`.
  def retry_transaction
    raise Transaction::RetryError.new
  end

  # Run two alternative transactional blocks - see `Concurrent::atomically`.
  # The `first` block is run; if it calls `Concurrent::retry_transaction`
  # its writes are rolled back (its reads are kept) and the `second` block
  # is run instead. If both blocks retry, the whole transaction retries and
  # sleeps until any `TVar` read by *either* branch changes.
  #
  # Must be called from within a `Concurrent::atomically` block.
  def or_else(first, second)
    transaction = Transaction::current
    raise ArgumentError.new('or_else must be called from within Concurrent::atomically') if transaction.nil?

    checkpoint = transaction.checkpoint

    begin
      first.call
    rescue Transaction::RetryError
      # The first branch cannot proceed - undo its writes (its reads stay
      # in the read set so the transaction still sleeps on them if the
      # second branch also retries) and try the alternative.

      transaction.rollback_to(checkpoint)

      begin
        second.call
      rescue Transaction::RetryError
        transaction.rollback_to(checkpoint)
        raise
      end
    end
  end

  module_function :atomically, :abort_transaction, :leave_transaction, :retry_transaction, :or_else

  private

  # @!visibility private
  class Transaction

    ABORTED = ::Object.new

    OpenEntry = Struct.new(:value, :modified)

    AbortError = Class.new(StandardError)
    LeaveError = Class.new(StandardError)
    RetryError = Class.new(StandardError)

    # Raised when `Concurrent::retry_transaction` is used in a transaction
    # that has not read any `TVar` and the enclosing `atomically` has no
    # timeout - nothing could ever wake it up.
    InvalidRetryError = Class.new(Concurrent::Error)

    # One-shot rendezvous a sleeping transaction waits on. Any `TVar` in
    # the transaction's read set signals it when a committed transaction
    # modifies that `TVar`.
    class Waiter
      def initialize
        @lock = Mutex.new
        @condition = ConditionVariable.new
        @signalled = false
      end

      # Sleep until signalled or `timeout` seconds pass. Returns true when
      # woken by a signal, false on timeout.
      def wait(timeout)
        @lock.synchronize do
          if timeout
            @condition.wait(@lock, timeout) unless @signalled
          else
            @condition.wait(@lock) unless @signalled
          end
          @signalled
        end
      end

      def signal
        @lock.synchronize do
          @signalled = true
          @condition.broadcast
        end
      end
    end

    def initialize
      @open_tvars = {}
      @read_set = []
      @undo_log = []
    end

    def read(tvar)
      entry = open(tvar)
      @read_set << tvar unless @read_set.include?(tvar)
      entry.value
    end

    def write(tvar, value)
      entry = open(tvar)
      @undo_log << [:write, tvar, entry.value, entry.modified]
      entry.modified = true
      entry.value = value
    end

    def open(tvar)
      entry = @open_tvars[tvar]

      unless entry
        unless tvar.unsafe_lock.try_lock
          Concurrent::abort_transaction
        end

        entry = OpenEntry.new(tvar.unsafe_value, false)
        @open_tvars[tvar] = entry
        @undo_log << [:open, tvar]
      end

      entry
    end

    # Remember the current position in the undo log so that a failed
    # `Concurrent::or_else` branch can be rolled back to this point.
    def checkpoint
      @undo_log.length
    end

    # Undo everything the transaction did since `checkpoint`: writes are
    # reverted and `TVar`s opened since then are released. Reads are
    # deliberately kept in the read set - `Concurrent::or_else` must sleep
    # on the union of the read sets of both of its branches.
    def rollback_to(checkpoint)
      while @undo_log.length > checkpoint
        action = @undo_log.pop

        case action[0]
        when :open
          tvar = action[1]
          @open_tvars.delete(tvar)
          tvar.unsafe_lock.unlock
        when :write
          _, tvar, value, modified = action
          entry = @open_tvars[tvar]
          entry.value = value
          entry.modified = modified
        end
      end
    end

    def abort
      unlock
    end

    def commit
      @open_tvars.each do |tvar, entry|
        if entry.modified
          tvar.unsafe_value = entry.value
          # Wake transactions sleeping in `retry_transaction` - still
          # holding the lock, so no sleeper can miss this write.
          tvar.unsafe_signal_waiters
        end
      end

      unlock

      true
    end

    # Release all locks and sleep until a committed transaction modifies a
    # `TVar` in the read set (or `timeout` seconds pass). The waiter is
    # registered while the locks are still held, closing the window between
    # "the condition was checked" and "the transaction went to sleep" in
    # which a write could otherwise be lost.
    def wait_for_change(timeout)
      if timeout && timeout <= 0
        unlock
        raise Concurrent::TimeoutError.new('timed out waiting for a TVar in the read set to change')
      end

      if @read_set.empty?
        unlock

        if timeout.nil?
          raise InvalidRetryError.new('cannot retry a transaction that has read nothing and has no timeout - it would sleep forever')
        else
          # Nothing can wake this transaction - just sleep out the deadline.
          sleep(timeout)
          raise Concurrent::TimeoutError.new('timed out waiting for a TVar in the read set to change')
        end
      end

      waiter = Waiter.new
      @read_set.each { |tvar| tvar.unsafe_add_waiter(waiter) }
      unlock

      begin
        signalled = waiter.wait(timeout)
      ensure
        @read_set.each do |tvar|
          tvar.unsafe_lock.synchronize { tvar.unsafe_remove_waiter(waiter) }
        end
      end

      unless signalled
        raise Concurrent::TimeoutError.new('timed out waiting for a TVar in the read set to change')
      end
    end

    def unlock
      @open_tvars.each_key do |tvar|
        tvar.unsafe_lock.unlock
      end
    end

    def self.current
      Thread.current[:current_tvar_transaction]
    end

    def self.current=(transaction)
      Thread.current[:current_tvar_transaction] = transaction
    end

  end

end
