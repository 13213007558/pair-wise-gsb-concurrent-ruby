require 'set'
require 'concurrent/synchronization/object'

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
      @version = 0
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
    def unsafe_version # :nodoc:
      @version
    end

    # @!visibility private
    def unsafe_version=(version) # :nodoc:
      @version = version
    end

    # @!visibility private
    def unsafe_lock # :nodoc:
      @lock
    end

    # @!visibility private
    def unsafe_waiters # :nodoc:
      @waiters
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
  # @example
  #   a = new TVar(100_000)
  #   b = new TVar(100)
  #
  #   Concurrent::atomically do
  #     a.value -= 10
  #     b.value += 10
  #   end
  def atomically
    raise ArgumentError.new('no block given') unless block_given?

    # Get the current transaction

    transaction = Transaction::current

    # Are we not already in a transaction (not nested)?

    if transaction.nil?
      # New transaction

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
            # Sleep until a read TVar changes, then run the block again
            transaction.retry_wait
            result = Transaction::ABORTED
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

  # Put the current transaction to sleep until one of the `TVar`s it has read
  # is changed by another transaction - see `Concurrent::atomically`.
  #
  # Use this when a transaction cannot make progress because of the values it
  # has read - for example an account balance that is too low. Instead of
  # aborting and busy-spinning, the transaction releases its locks, registers
  # itself as a waiter on every `TVar` it has read, and sleeps. When any of
  # those `TVar`s is modified by a committed transaction the sleeper is woken
  # and the whole transaction is re-run from the start.
  #
  # Registering the waiters and checking that no read `TVar` has changed is
  # done while the transaction still holds its locks, so a change that happens
  # just as the transaction is going to sleep can never be missed.
  #
  # A transaction that has read nothing can never be woken, so in that case
  # `Concurrent::Transaction::RetryError` is raised out of
  # `Concurrent::atomically` instead of sleeping forever.
  #
  # Inside `Concurrent::or_else` a retry does not put the transaction to
  # sleep - it abandons the left branch and tries the right one.
  def retry_transaction
    raise Transaction::RetryError.new
  end

  # Run the left branch of a transaction and, if it retries, discard its
  # writes and run the right branch instead - see `Concurrent::atomically` and
  # `Concurrent::retry_transaction`.
  #
  # Takes two callable branches, either as two lambdas or as a lambda and a
  # block. If the left branch calls `Concurrent::retry_transaction` its
  # writes are rolled back - but the `TVar`s it read stay in the transaction's
  # read set - and the right branch is run. If the right branch also retries,
  # the whole transaction sleeps until any `TVar` read by either branch is
  # changed.
  #
  # Only retries are caught - `Concurrent::abort_transaction` and other
  # exceptions raised by either branch abort the whole transaction as usual.
  #
  # @example
  #   Concurrent::atomically do
  #     Concurrent::or_else(
  #       -> { Concurrent::retry_transaction if inbox.value.empty? },
  #       -> { :nothing_waiting }
  #     )
  #   end
  def or_else(left, right = nil, &block)
    right ||= block
    raise ArgumentError.new('or_else requires two branches') if left.nil? || right.nil?

    transaction = Transaction::current

    if transaction.nil?
      Concurrent::atomically { Concurrent::or_else(left, right) }
    else
      transaction.or_else(left, right)
    end
  end

  module_function :atomically, :abort_transaction, :leave_transaction, :retry_transaction, :or_else

  private

  # @!visibility private
  class Transaction

    ABORTED = ::Object.new

    OpenEntry = Struct.new(:value, :modified, :read, :version)

    AbortError = Class.new(StandardError)
    LeaveError = Class.new(StandardError)
    RetryError = Class.new(StandardError)

    # Guards the waiter lists of all `TVar`s. Always acquired after any
    # `TVar` locks, never before, so the lock ordering cannot deadlock.
    WAITERS_LOCK = Mutex.new

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

        entry = OpenEntry.new(tvar.unsafe_value, false, false, tvar.unsafe_version)
        @open_tvars[tvar] = entry
      end

      entry
    end

    # Run the left branch. If it retries, roll back only its writes - reads
    # made by the left branch stay in the read set so that a transaction in
    # which both branches retry sleeps on the union of both read sets - and
    # run the right branch.
    def or_else(left, right)
      checkpoint = {}
      @open_tvars.each do |tvar, entry|
        checkpoint[tvar] = [entry.value, entry.modified]
      end

      begin
        left.call
      rescue RetryError
        rollback_writes(checkpoint)
        right.call
      end
    end

    def abort
      unlock
    end

    def commit
      @open_tvars.each do |tvar, entry|
        if entry.modified
          tvar.unsafe_value = entry.value
          tvar.unsafe_version += 1
        end
      end

      waiters = []
      WAITERS_LOCK.synchronize do
        @open_tvars.each do |tvar, entry|
          if entry.modified
            waiters.concat(tvar.unsafe_waiters)
            tvar.unsafe_waiters.clear
          end
        end
      end

      unlock

      waiters.each { |waiter| waiter << true }

      true
    end

    # Sleep until another transaction commits a change to a `TVar` read by
    # this transaction. The locks on all read `TVar`s are still held on
    # entry, so no committer can slip a change in between the version check
    # and the waiter registration - a change can never be missed.
    def retry_wait
      read_tvars = []
      @open_tvars.each do |tvar, entry|
        read_tvars << tvar if entry.read
      end

      if read_tvars.empty?
        unlock
        raise RetryError, 'cannot retry a transaction that has read no TVars - it would sleep forever'
      end

      queue = Queue.new

      registered = WAITERS_LOCK.synchronize do
        if read_tvars.all? { |tvar| tvar.unsafe_version == @open_tvars[tvar].version }
          read_tvars.each { |tvar| tvar.unsafe_waiters << queue }
          true
        else
          false
        end
      end

      unlock

      if registered
        queue.pop

        WAITERS_LOCK.synchronize do
          read_tvars.each { |tvar| tvar.unsafe_waiters.delete(queue) }
        end
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

    private

    # Undo writes made since the checkpoint, keeping the read set. Entries
    # created by the failed branch still hold their `TVar` lock, so the
    # committed value is exactly the value the branch first saw.
    def rollback_writes(checkpoint)
      @open_tvars.each do |tvar, entry|
        saved = checkpoint[tvar]

        if saved
          entry.value = saved[0]
          entry.modified = saved[1]
        else
          entry.value = tvar.unsafe_value
          entry.modified = false
        end
      end
    end

  end

end
