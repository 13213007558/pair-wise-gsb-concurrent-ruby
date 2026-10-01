require 'concurrent/errors'
require 'concurrent/synchronization/lockable_object'
require 'concurrent/promises'

module Concurrent

  # The TaskScope abstraction provides structured concurrency for a group of
  # related tasks.
  #
  # A request handler (or any other unit of work) often starts several
  # parallel sub-tasks. When the handler is done it wants all of them to be
  # finished or cancelled, not scattered around a shared thread pool where
  # they keep running after the handler has moved on. A task scope confines
  # the lifetime of the sub-tasks to a block:
  #
  #   Concurrent::TaskScope.run do |scope|
  #     user   = scope.spawn { |token| fetch_user(token) }
  #     orders = scope.spawn { |token| fetch_orders(token) }
  #     [scope.wait(user), scope.wait(orders)]
  #   end
  #
  # Guarantees:
  #
  # *   When the block exits (normally or with an error) the scope waits
  #     until every task it has admitted is finished. The block never
  #     returns while one of its tasks is still running.
  # *   When a task fails, the scope's {#cancellation} token is cancelled so
  #     the sibling tasks are asked to stop cooperatively. Cancellation is
  #     cooperative only: tasks observe it at explicit checkpoints
  #     ({CancellationToken#check!} or {CancellationToken#canceled?}),
  #     threads are never killed or raised into.
  # *   Failures of the admitted tasks are collected. If more than one task
  #     fails, each failure reason is preserved and reported together in
  #     {Failure#errors} when the block exits.
  # *   A nested scope only tracks the tasks spawned into it. It cannot
  #     cancel or observe tasks of an outer scope, and no scope ever shuts
  #     down the (possibly shared) executor its tasks run on.
  #
  # Because cancellation is cooperative, a task which does not check the
  # token keeps running and the scope will wait for it. The scope therefore
  # never reports itself complete while an admitted task is still running.
  class TaskScope < Synchronization::LockableObject
    safe_initialization!

    # Raised by {TaskScope.run} when one or more of the tasks admitted to
    # the scope failed. All failure reasons are preserved, see {#errors}.
    class Failure < Concurrent::Error
      # @return [Array<StandardError>] the reasons each failed task was
      #   rejected with, in the order the tasks were admitted to the scope
      attr_reader :errors

      def initialize(errors)
        @errors = errors.dup.freeze
        super format('%d task(s) failed: %s',
                     errors.size,
                     errors.map { |error| "#{error.class}: #{error.message}" }.join('; '))
      end
    end

    # A cooperative cancellation token shared by all tasks of a {TaskScope}.
    #
    # The standard methods `Thread#raise` and `Thread#kill` available in
    # Ruby are very dangerous, therefore the token is only a request: a task
    # has to check it at explicit checkpoints and stop on its own.
    # Good practices to make tasks cancellable:
    # * check the token every cycle of a loop which does significant work,
    # * do blocking actions in a loop with a timeout, checking the token
    #   on each timeout.
    class CancellationToken
      # @api private
      def initialize(origin)
        @Origin = origin
      end

      # Has cancellation been requested?
      # @return [true, false]
      def canceled?
        @Origin.resolved?
      end

      # Raise an error when cancellation was requested. Intended to be
      # called at explicit checkpoints of the task.
      # @param [#exception] error to be raised
      # @raise the error
      # @return [self]
      def check!(error = CancelledOperationError)
        raise error if canceled?
        self
      end

      # Short string representation.
      # @return [String]
      def to_s
        format '%s %s>', super()[0..-2], canceled? ? 'canceled' : 'pending'
      end

      alias_method :inspect, :to_s
    end

    # Creates a scope, yields it to the block, and waits for all admitted
    # tasks before returning.
    #
    # @param [Hash] opts the options used to define the scope
    # @option opts [Executor] :executor (:io) the executor the tasks are
    #   scheduled on. Three special values are also supported: `:io` returns
    #   the global pool for long, blocking (IO) tasks, `:fast` returns the
    #   global pool for short, fast operations, and `:immediate` returns the
    #   global `ImmediateExecutor` object. The executor is never shut down
    #   by the scope.
    # @yield [scope] the body of the scope
    # @yieldparam [TaskScope] scope the scope to spawn tasks into
    # @return [Object] the value of the block
    # @raise [Failure] when the block finished but one or more tasks failed
    def self.run(*args, &block)
      new(*args).run(&block)
    end

    # Creates a new task scope. Prefer {.run} which ties the scope's
    # lifetime to a block.
    #
    # @param [Hash] opts the options used to define the scope
    # @option opts [Executor] :executor (:io) the executor the tasks are
    #   scheduled on
    def initialize(executor: :io)
      super()
      @Executor     = Concurrent.executor(executor)
      @Origin       = Promises.resolvable_event
      @Cancellation = CancellationToken.new(@Origin)
      @Tasks        = []
      @Errors       = []
      @Open         = true
      @Started      = false
    end

    # The cancellation token shared by all tasks of this scope. It is
    # cancelled when any task of the scope fails or when the block passed
    # to {#run} raises. Tasks observe it cooperatively at explicit
    # checkpoints.
    # @return [CancellationToken]
    def cancellation
      @Cancellation
    end

    # Admits a new task to the scope and schedules it on the scope's
    # executor.
    #
    # @param [Array<Object>] args passed to the task after the cancellation
    #   token
    # @yield [cancellation, *args] the task body. The first argument is the
    #   scope's {CancellationToken}; the task should check it at explicit
    #   checkpoints to support cooperative cancellation.
    # @return [Promises::Future] a future representing the task's result
    # @raise [Concurrent::Error] when the scope is already closed
    def spawn(*args, &task)
      raise ArgumentError, 'a block is required' unless task

      synchronize do
        raise Concurrent::Error, 'cannot spawn a task into a scope which is already closed' unless @Open

        future = Promises.future_on(@Executor, @Cancellation, *args) do |cancellation, *task_args|
          task.call(cancellation, *task_args)
        end
        # A failed task asks its siblings to stop cooperatively. Runs on the
        # resolving thread, so the token is cancelled as soon as possible.
        future.on_rejection! { @Origin.resolve }
        @Tasks << future
        future
      end
    end

    # Waits for the given task of this scope and returns its value.
    # @param [Promises::Future] task a future returned by {#spawn}
    # @return [Object] the task's value
    # @raise the reason the task was rejected with
    # @raise [ArgumentError] when the task does not belong to this scope
    def wait(task)
      unless synchronize { @Tasks.include? task }
        raise ArgumentError, 'the task does not belong to this scope'
      end
      task.value!
    end

    # Runs the block and waits for all admitted tasks to finish.
    # @see TaskScope.run
    def run
      raise ArgumentError, 'a block is required' unless block_given?
      synchronize do
        raise Concurrent::Error, 'this scope has already been run' if @Started
        @Started = true
      end

      block_error = nil
      result      = nil
      begin
        result = yield self
      rescue Exception => error # rubocop:disable Lint/RescueException -- re-raised after cleanup
        block_error = error
        # Ask the still running tasks to stop cooperatively.
        @Origin.resolve
      end

      errors = close_and_collect

      raise block_error if block_error
      raise Failure.new(errors) unless errors.empty?
      result
    end

    # Are all tasks admitted so far finished?
    #
    # Note the scope only reports itself complete when every admitted task
    # is actually resolved. A task which ignores the cancellation token
    # until its next checkpoint keeps the scope incomplete, even after the
    # token was cancelled.
    # @return [true, false]
    def complete?
      synchronize { @Tasks.all?(&:resolved?) }
    end

    # Is the scope still accepting new tasks?
    # @return [true, false]
    def open?
      synchronize { @Open }
    end

    # The number of tasks admitted to the scope so far.
    # @return [Integer]
    def size
      synchronize { @Tasks.size }
    end

    # The failure reasons collected from the admitted tasks so far.
    # @return [Array<StandardError>]
    def errors
      synchronize { @Errors.dup }
    end

    # Short string representation.
    # @return [String]
    def to_s
      format '%s %s, %d task(s)>',
             super()[0..-2],
             open? ? 'open' : 'closed',
             size
    end

    alias_method :inspect, :to_s

    private

    # Stops accepting new tasks, waits until every admitted task is
    # finished, and collects the failure reasons preserving each task's own
    # reason.
    # @return [Array<StandardError>] the collected failure reasons
    def close_and_collect
      tasks = synchronize do
        @Open = false
        @Tasks.dup
      end
      # Never kill or raise into the task threads; wait until each task
      # reaches a checkpoint and finishes on its own.
      tasks.each(&:wait)
      synchronize do
        tasks.each { |task| @Errors << task.reason if task.rejected? }
        @Errors.dup
      end
    end
  end
end
