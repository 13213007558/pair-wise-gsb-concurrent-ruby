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
    # Register a `Transaction::RetryWaiter` to be signalled when this `TVar`
    # is changed by a committed transaction. The caller must hold this
    # `TVar`'s lock, which is what makes the registration race-free: the
    # committing thread takes the same lock before signalling waiters.
    def add_waiter(waiter) # :nodoc:
      @waiters << waiter
    end

    # @!visibility private
    # Wake every transaction sleeping on this `TVar`. The caller must hold
    # this `TVar`'s lock.
    def notify_waiters # :nodoc:
      waiters = @waiters
      @waiters = []
      waiters.each(&:signal)
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
  # * `Concurrent::retry_transaction` puts the transaction to sleep until one
  #     of the `TVar`s it has read so far changes, and then re-runs the whole
  #     transaction from the start. Retrying with an empty read set raises
  #     `Transaction::RetryError`, as such a transaction could never be woken.
  #
  # * `Concurrent::or_else` composes two alternative computations: if the
  #     first one retries, its writes are rolled back and the second one runs;
  #     if both retry, the transaction sleeps on the union of both read sets.
  #
  # Transactions within transactions are flattened to a single transaction, so
  # a retry or abort anywhere inside always applies to the outermost
  # transaction.
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
          rescue Transaction::RetryError
            # Sleep until a TVar this transaction read changes, then loop
            # around and re-run the whole transaction from the start.
            transaction.await_change
            result = Transaction::ABORTED
          rescue Transaction::AbortError
            transaction.abort
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

  # Put the currently running transaction to sleep until one of the `TVar`s
  # it has read so far is changed by another committed transaction, and then
  # re-run the whole transaction from the start - see `Concurrent::atomically`.
  #
  # Unlike `Concurrent::abort_transaction`, which busy-loops the transaction,
  # `retry_transaction` blocks the thread without consuming CPU, which makes
  # it the right way to wait for a condition, such as an account balance
  # becoming high enough:
  #
  #   Concurrent::atomically do
  #     Concurrent::retry_transaction if account.value < amount
  #     account.value -= amount
  #   end
  #
  # The transaction only wakes when a `TVar` it actually read changes, so
  # reads matter: a transaction that retries without having read any `TVar`
  # could never be woken, and `Concurrent::atomically` therefore raises
  # `Transaction::RetryError` instead of sleeping forever. Any writes made
  # before retrying are discarded, and the transaction is re-run in full.
  #
  # Outside an `atomically` block the `Transaction::RetryError` is simply
  # raised to the caller.
  def retry_transaction
    raise Transaction::RetryError.new
  end

  # Compose two alternative transactional computations - see
  # `Concurrent::atomically` and `Concurrent::retry_transaction`.
  #
  # Runs `left` within the current transaction. If `left` calls
  # `Concurrent::retry_transaction`, its writes are rolled back and `right`
  # runs instead. If `right` also retries, the transaction sleeps until any
  # `TVar` read by *either* alternative changes, and the whole transaction
  # then re-runs from the start. The return value is whichever alternative's
  # value completed without retrying.
  #
  # Any exception other than `Transaction::RetryError` raised by either
  # alternative aborts the whole transaction and propagates, as does
  # `Concurrent::abort_transaction` and `Concurrent::leave_transaction`.
  #
  # Must be called from within `Concurrent::atomically`.
  #
  # @example
  #   Concurrent::atomically do
  #     Concurrent::or_else(
  #       -> { Concurrent::retry_transaction if queue_a.value.empty?; queue_a.value.shift },
  #       -> { Concurrent::retry_transaction if queue_b.value.empty?; queue_b.value.shift }
  #     )
  #   end
  def or_else(left, right)
    unless left.respond_to?(:call) && right.respond_to?(:call)
      raise ArgumentError.new('or_else requires two callable alternatives')
    end

    transaction = Transaction::current

    if transaction.nil?
      raise ArgumentError.new('or_else must be called from within Concurrent::atomically')
    end

    transaction.or_else(left, right)
  end

  module_function :atomically, :abort_transaction, :leave_transaction, :retry_transaction, :or_else

  private

  # @!visibility private
  class Transaction

    ABORTED = ::Object.new

    OpenEntry = Struct.new(:value, :modified, :read)

    AbortError = Class.new(StandardError)
    LeaveError = Class.new(StandardError)
    RetryError = Class.new(StandardError)

    # A one-shot rendezvous between a committing transaction and a single
    # transaction sleeping in `Concurrent::retry_transaction`. `signal` may
    # be called before or after `wait`; the flag makes sure a signal that
    # arrives in the gap between `add_waiter` and `wait` is not lost.
    class RetryWaiter # :nodoc:
      def initialize
        @mutex    = Mutex.new
        @resource = ConditionVariable.new
        @signaled = false
      end

      def signal
        @mutex.synchronize do
          @signaled = true
          @resource.broadcast
        end
      end

      def wait
        @mutex.synchronize do
          @resource.wait(@mutex) unless @signaled
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

    # Run `left`; if it retries, roll its writes back and run `right`.
    # Reads from both alternatives stay in the transaction's read set, so if
    # both retry the transaction waits on the union of their reads.
    def or_else(left, right)
      checkpoint = take_checkpoint

      begin
        left.call
      rescue RetryError
        restore_checkpoint(checkpoint)

        checkpoint = take_checkpoint

        begin
          right.call
        rescue RetryError
          restore_checkpoint(checkpoint)
          raise
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
          # Wake transactions sleeping on this TVar. The lock is still held,
          # so no sleeper can register a waiter after this point without
          # first reading the new value.
          tvar.notify_waiters
        end
      end

      unlock
    end

    # Sleep until any TVar read by this transaction is changed by a commit.
    # The transaction still holds the locks of every TVar it opened, and
    # committers take those same locks before changing values and signalling
    # waiters, so registering the waiter and releasing the locks cannot miss
    # a change.
    def await_change
      read_tvars = []
      @open_tvars.each do |tvar, entry|
        read_tvars << tvar if entry.read
      end

      if read_tvars.empty?
        unlock
        raise RetryError.new('retry_transaction called with an empty read set - the transaction could never be woken')
      end

      waiter = RetryWaiter.new
      read_tvars.each { |tvar| tvar.add_waiter(waiter) }
      unlock
      waiter.wait
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

    def take_checkpoint
      checkpoint = {}
      @open_tvars.each do |tvar, entry|
        checkpoint[tvar] = entry.dup
      end
      checkpoint
    end

    # Undo everything an `or_else` alternative did, while keeping its reads
    # in the transaction's read set so it can be woken by their changes.
    def restore_checkpoint(checkpoint)
      @open_tvars.each do |tvar, entry|
        saved = checkpoint[tvar]

        if saved
          entry.value    = saved.value
          entry.modified = saved.modified
          entry.read     = saved.read || entry.read
        elsif entry.read
          # Read by the failed alternative: forget any writes but keep
          # watching the TVar so the transaction can wait on it.
          entry.value    = tvar.unsafe_value
          entry.modified = false
        else
          # Only written by the failed alternative: drop it entirely.
          @open_tvars.delete(tvar)
          tvar.unsafe_lock.unlock
        end
      end
    end

  end

end
