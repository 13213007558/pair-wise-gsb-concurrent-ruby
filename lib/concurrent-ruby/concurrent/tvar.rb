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

    # @!visibility private
    def unsafe_add_waiter(waiter) # :nodoc:
      @waiters << waiter
    end

    # @!visibility private
    def unsafe_remove_waiter(waiter) # :nodoc:
      @waiters.delete(waiter)
    end

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
  # A transaction may call `Concurrent::retry_transaction` when a condition it
  # depends on does not hold yet. Instead of busy-waiting, the transaction is
  # then put to sleep - after releasing all of its locks - and is only run
  # again once one of the `TVar`s it has read has been modified by another
  # transaction. Writes to `TVar`s the transaction did not read do not wake it
  # up. Use `Concurrent::or_else` to try an alternative transaction when the
  # first one retries.
  #
  # @param [Numeric, nil] timeout limits how long the whole `atomically` call
  #   may spend sleeping in `Concurrent::retry_transaction`, in seconds. When
  #   the time is up a `Transaction::RetryTimeoutError` is raised. `nil` (the
  #   default) waits without a limit.
  #
  # @example
  #   a = new TVar(100_000)
  #   b = new TVar(100)
  #
  #   Concurrent::atomically do
  #     a.value -= 10
  #     b.value += 10
  #   end
  def atomically(timeout = nil)
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
          rescue Transaction::RetryError
            # Sleep until a TVar we read changes, then run the block again

            wait_for_retry(transaction, deadline)
            next
          rescue Transaction::LeaveError
            transaction.abort
            break result
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

  # Retry the currently running transaction - see `Concurrent::atomically`.
  #
  # This is used when a transaction finds that the condition it was waiting
  # for does not (yet) hold. The transaction is rolled back, its locks are
  # released, and the calling thread goes to sleep instead of busy-waiting.
  # The transaction is run again once any `TVar` it read has been changed by
  # another transaction, so it is never woken up by writes it cannot observe.
  def retry_transaction
    raise Transaction::RetryError.new
  end

  # Run the first transaction, and if it retries, run the second one instead -
  # see `Concurrent::atomically`.
  #
  # Both arguments are callables (usually lambdas) that contain transactional
  # code. If `first` calls `Concurrent::retry_transaction`, the writes it made
  # are rolled back and `second` is run. The `TVar`s read by the failed branch
  # stay in the transaction's read set, so if both branches retry the
  # transaction sleeps until a `TVar` read by either branch changes. `or_else`
  # may be nested, and may also be called outside of `Concurrent::atomically`,
  # in which case it is wrapped in a transaction of its own.
  def or_else(first, second)
    transaction = Transaction::current

    if transaction.nil?
      Concurrent::atomically do
        Transaction::current.or_else(first, second)
      end
    else
      transaction.or_else(first, second)
    end
  end

  module_function :atomically, :abort_transaction, :leave_transaction,
                  :retry_transaction, :or_else

  # @!visibility private
  def wait_for_retry(transaction, deadline) # :nodoc:
    read_set = transaction.read_set
    remaining = deadline && deadline - Concurrent.monotonic_time

    if read_set.empty?
      # Nothing to wait on - without a timeout we would sleep forever, with
      # one we simply sleep it out and then give up.

      transaction.abort

      if remaining.nil?
        raise ArgumentError.new('cannot retry a transaction with an empty read set and no timeout - it would sleep forever')
      else
        sleep remaining if remaining > 0
        raise Transaction::RetryTimeoutError.new('transaction retry timed out')
      end
    end

    if remaining && remaining <= 0
      transaction.abort
      raise Transaction::RetryTimeoutError.new('transaction retry timed out')
    end

    # Register the waiter while we still hold the locks on everything we
    # read. A committing transaction can only modify one of those TVars
    # after we release the locks, and by then it is guaranteed to find and
    # signal our waiter - so no write can be lost between checking the
    # condition and going to sleep.

    waiter = Transaction::Waiter.new
    read_set.each { |tvar| tvar.unsafe_add_waiter(waiter) }
    transaction.abort

    signaled = waiter.wait(remaining)

    # Remove the waiter again wherever it has not already been consumed by
    # a committing transaction.

    read_set.each do |tvar|
      tvar.unsafe_lock.synchronize { tvar.unsafe_remove_waiter(waiter) }
    end

    unless signaled
      raise Transaction::RetryTimeoutError.new('transaction retry timed out')
    end
  end
  module_function :wait_for_retry

  private

  # @!visibility private
  class Transaction

    ABORTED = ::Object.new

    OpenEntry = Struct.new(:value, :modified, :read)

    AbortError = Class.new(StandardError)
    LeaveError = Class.new(StandardError)
    RetryError = Class.new(StandardError)
    RetryTimeoutError = Class.new(Concurrent::TimeoutError)

    # @!visibility private
    class Waiter # :nodoc:
      def initialize
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @signaled = false
      end

      def signal
        @mutex.synchronize do
          @signaled = true
          @condition.broadcast
        end
      end

      # Returns true when signaled, false when the timeout expired first.
      def wait(timeout = nil)
        @mutex.synchronize do
          if timeout
            deadline = Concurrent.monotonic_time + timeout
            until @signaled
              remaining = deadline - Concurrent.monotonic_time
              break if remaining <= 0
              @condition.wait(@mutex, remaining)
            end
          else
            @condition.wait(@mutex) until @signaled
          end
          @signaled
        end
      end
    end

    def initialize
      @open_tvars = {}
    end

    def read(tvar)
      entry = open(tvar)
      entry.read = true
      entry.value
    end

    def write(tvar, value)
      entry = open(tvar)
      entry.modified = true
      entry.value = value
    end

    def open(tvar)
      entry = @open_tvars[tvar]

      unless entry
        unless tvar.unsafe_lock.try_lock
          Concurrent::abort_transaction
        end

        entry = OpenEntry.new(tvar.unsafe_value, false, false)
        @open_tvars[tvar] = entry
      end

      entry
    end

    # The TVars whose committed values this transaction has observed - the
    # set a top-level retry sleeps on.
    def read_set
      set = []
      @open_tvars.each do |tvar, entry|
        set << tvar if entry.read
      end
      set
    end

    # Run `first`; if it retries, roll back the writes it made and run
    # `second` instead. Reads made by the failed branch are kept in the
    # transaction's read set, so a surrounding retry still waits on them.
    def or_else(first, second)
      checkpoint = {}
      @open_tvars.each { |tvar, entry| checkpoint[tvar] = entry.dup }

      begin
        first.call
      rescue RetryError
        rollback_to(checkpoint)
        second.call
      end
    end

    def abort
      unlock
    end

    def commit
      @open_tvars.each do |tvar, entry|
        if entry.modified
          tvar.unsafe_value = entry.value
          tvar.unsafe_signal_waiters
        end
      end

      unlock
      true
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

    private

    # Undo a failed `or_else` branch. Entries the branch only read stay in
    # the read set; entries it wrote are discarded and unlocked again, and
    # entries that predate the branch are restored to their old state.
    def rollback_to(checkpoint)
      kept = {}

      @open_tvars.each do |tvar, entry|
        if checkpoint.key?(tvar)
          kept[tvar] = checkpoint[tvar]
        elsif entry.read && !entry.modified
          kept[tvar] = entry
        else
          tvar.unsafe_lock.unlock
        end
      end

      @open_tvars = kept
    end

  end

end
