require 'concurrent/errors'
require 'concurrent/options'
require 'concurrent/synchronization/lockable_object'
require 'concurrent/edge/cancellation'

module Concurrent

  # The TaskScope abstraction provides structured concurrency for a block of
  # work which needs to run several subtasks in parallel.
  #
  # A scope is opened with {.open} which yields a scope object. Subtasks are
  # admitted with {#spawn} and run on a shared {ExecutorService} (the global
  # `:io` executor by default). When the block exits, the scope waits until
  # every admitted subtask has finished — no task is left behind running on
  # the shared thread pool. The scope never shuts the executor down; nested
  # scopes only ever manage their own tasks.
  #
  # When a subtask fails (raises an error other than {CancelledOperationError})
  # the remaining subtasks are notified through a {Cancellation} token so they
  # can stop cooperatively. Cancellation is cooperative only: subtasks observe
  # it at explicit checkpoints ({Cancellation#check!} or {Cancellation#canceled?}),
  # threads are never killed or raised into. A subtask which ignores the
  # cancellation until its next checkpoint keeps the scope open — the scope
  # does not report itself complete until every admitted task has actually
  # finished.
  #
  # On exit the results of all admitted subtasks are collected. If any subtask
  # failed, a {TaskGroupError} is raised which preserves each individual
  # failure reason in {#failures}.
  #
  # @example
  #   scope = Concurrent::TaskScope.open do |scope|
  #     scope.spawn { |token| fetch(token, :users) }
  #     scope.spawn { |token| fetch(token, :orders) }
  #   end
  #   scope.results # => [[true, users, nil], [true, orders, nil]]
  #
  # @example nested scopes propagate cancellation but manage only their own tasks
  #   Concurrent::TaskScope.open do |outer|
  #     outer.spawn do |token|
  #       Concurrent::TaskScope.open(cancellation: token) do |inner|
  #         inner.spawn { |inner_token| ... }
  #       end
  #     end
  #   end
  #
  # @!macro warn.edge
  class TaskScope < Synchronization::LockableObject
    safe_initialization!

    # Raised when one or more child tasks of a {TaskScope} failed.
    # Every individual failure reason is preserved in {#failures}.
    class TaskGroupError < Concurrent::Error
      # @return [Array<Exception>] the reasons of all failed child tasks,
      #   in the order the failures were observed
      attr_reader :failures

      def initialize(failures)
        @failures = failures.freeze
        super format('%d child task(s) failed: %s',
                     failures.size,
                     failures.map { |failure| "#{failure.class}: #{failure.message}" }.join('; '))
      end
    end

    # A handle to a single subtask admitted to a {TaskScope}.
    class Task < Synchronization::LockableObject
      safe_initialization!

      def initialize
        super()
        @done    = false
        @success = false
        @value   = nil
        @reason  = nil
      end

      # Has the task finished (successfully, by failure, or by cancellation)?
      # @return [true, false]
      def done?
        synchronize { @done }
      end

      # Did the task finish with a value?
      # @return [true, false]
      def success?
        synchronize { @done && @success }
      end

      # Was the task cancelled cooperatively (it observed the cancellation
      # token at a checkpoint, or never started because the scope was
      # already cancelled)?
      # @return [true, false]
      def cancelled?
        synchronize { @done && !@success && @reason.is_a?(CancelledOperationError) }
      end

      # Did the task fail with an error other than cooperative cancellation?
      # @return [true, false]
      def failed?
        synchronize { @done && !@success && !@reason.is_a?(CancelledOperationError) }
      end

      # Blocks until the task is done.
      # @param [Numeric, nil] timeout maximum time to wait in seconds, nil waits forever
      # @return [self]
      def wait(timeout = nil)
        synchronize { ns_wait(timeout) until @done }
        self
      end

      # Blocks until done, then returns the value the task produced.
      # @raise the reason the task failed or was cancelled with
      # @return [Object] the task's return value
      def value
        wait
        synchronize do
          raise @reason unless @success
          @value
        end
      end

      # Blocks until done, then returns the failure/cancellation reason.
      # @return [Exception, nil]
      def reason
        wait
        synchronize { @reason }
      end

      # Blocks until done, then returns a triplet describing the outcome.
      # @return [Array(true, Object, nil), Array(false, nil, Exception)]
      def result
        wait
        synchronize { [@success, @value, @reason] }
      end

      # @!visibility private
      def ns_complete(success, value, reason)
        synchronize do
          @success = success
          @value   = value
          @reason  = reason
          @done    = true
          ns_broadcast
        end
        self
      end
    end

    # Opens a new task scope, yields it, and on block exit waits until all
    # admitted subtasks have finished.
    #
    # @param [Hash] options
    # @option options [ExecutorService, :fast, :io, :immediate] :executor (:io)
    #   the shared executor subtasks run on. The executor is never shut down
    #   by the scope.
    # @option options [Cancellation, nil] :cancellation (nil) an optional
    #   parent cancellation token. When it is cancelled, this scope's subtasks
    #   are cancelled cooperatively as well. Useful for nested scopes.
    # @yieldparam [TaskScope] scope the scope to spawn subtasks into
    # @return [TaskScope] the completed scope; use {#results} or {#tasks}
    #   to inspect the collected outcomes
    # @raise [TaskGroupError] when one or more subtasks failed; every
    #   individual reason is preserved in {TaskGroupError#failures}
    # @raise the block's own error is re-raised when the block itself raises
    #   (subtasks are cancelled and collected first; if subtasks also failed
    #   a {TaskGroupError} is raised with the block's error as its `cause`)
    def self.open(options = {}, &block)
      scope       = new(options)
      block_error = nil
      begin
        yield scope
      rescue Exception => error
        block_error = error
      end
      scope.send :close_and_wait, cancel: !block_error.nil?
      failures = scope.failures
      if block_error
        raise block_error if failures.empty?
        raise TaskGroupError.new(failures), cause: block_error
      elsif !failures.empty?
        raise TaskGroupError.new(failures)
      end
      scope
    end

    # @!visibility private
    def initialize(options = {})
      super()
      @Executor     = Concurrent::Options.executor(options.fetch(:executor, :io))
      token, origin = Cancellation.new
      @CancelOrigin = origin
      parent        = options.fetch(:cancellation, nil)
      @Cancellation = parent ? token.join(parent) : token
      @open         = true
      @pending      = 0
      @cancelled    = false
      @tasks        = []
      @failures     = []
    end

    # Admits a new subtask to the scope. The block is called on the scope's
    # executor with the scope's cancellation token as its first argument,
    # followed by `args`. The task should observe the token at explicit
    # checkpoints to support cooperative cancellation.
    #
    # @param [Array] args additional arguments passed to the block after the
    #   cancellation token
    # @return [Task] a handle to the admitted task
    # @raise [LifecycleError] when the scope is already closed
    def spawn(*args, &block)
      raise ArgumentError, 'no block given' unless block
      task     = Task.new
      admitted = synchronize do
        if @open
          @tasks << task
          @pending += 1
          true
        else
          false
        end
      end
      raise LifecycleError, 'cannot spawn into a closed TaskScope' unless admitted
      post task, args, block
      task
    end

    # Cooperatively cancels all subtasks of this scope (and only those).
    # Running tasks observe the cancellation at their next checkpoint;
    # they are never killed.
    # @return [self]
    def cancel!
      synchronize { ns_cancel }
      self
    end

    # The cancellation token handed to every subtask of this scope.
    # @return [Cancellation]
    def cancellation
      @Cancellation
    end

    # Is the scope closed and are all admitted tasks finished?
    # A task which ignores cancellation until its next checkpoint keeps
    # this false — the scope never reports completion early.
    # @return [true, false]
    def complete?
      synchronize { !@open && @pending.zero? }
    end

    # @return [Array<Task>] handles of all admitted tasks, in spawn order
    def tasks
      synchronize { @tasks.dup }
    end

    # Collected results of all admitted tasks, in spawn order.
    # @return [Array<Array>] a `[success, value, reason]` triplet per task
    def results
      tasks.map(&:result)
    end

    # @return [Array<Exception>] reasons of all failed subtasks so far
    def failures
      synchronize { @failures.dup }
    end

    private

    def post(task, args, block)
      posted = @Executor.post { run_task task, args, block }
      complete_task task, false, nil,
                    RejectedExecutionError.new('executor rejected the task') unless posted
    end

    def run_task(task, args, block)
      if @Cancellation.canceled?
        complete_task task, false, nil, CancelledOperationError.new
        return
      end
      begin
        complete_task task, true, block.call(@Cancellation, *args), nil
      rescue Exception => error
        complete_task task, false, nil, error
      end
    end

    def complete_task(task, success, value, reason)
      task.ns_complete success, value, reason
      synchronize do
        @pending -= 1
        if reason && !reason.is_a?(CancelledOperationError)
          @failures << reason
          # one child failed -> notify the others to cancel cooperatively
          ns_cancel
        end
        ns_broadcast if @pending.zero?
      end
      self
    end

    def close_and_wait(cancel: false)
      synchronize do
        @open = false
        ns_cancel if cancel
        ns_wait until @pending.zero?
      end
      self
    end

    def ns_cancel
      unless @cancelled
        @cancelled = true
        @CancelOrigin.resolve
      end
      self
    end
  end
end
