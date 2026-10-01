require 'concurrent/errors'
require 'concurrent/atomic/event'
require 'concurrent/executor/abstract_executor_service'
require 'concurrent/executor/fixed_thread_pool'

module Concurrent

  # An executor which serializes tasks that carry the same *key* while running
  # tasks with different keys in parallel.
  #
  # Tasks posted with the same key are guaranteed to:
  #
  # * never run at the same time as each other, and
  # * run in the order in which they were posted.
  #
  # Tasks posted with different keys may run concurrently, up to the configured
  # number of worker threads. A single busy key can therefore never starve
  # other keys: whenever a key finishes a task it is moved to the back of a
  # single first-in-first-out "ready" queue, so any other queued keys get a
  # chance to run before the busy key is scheduled again.
  #
  # The parallelism is provided by one bounded, fixed-size thread pool which is
  # shared by every key. The total number of accepted-but-not-completed tasks
  # (waiting plus running) is capped by `:max_queue`, which bounds the amount of
  # work that may pile up regardless of how many distinct keys are used.
  #
  # Keys whose tasks have all completed are automatically removed, so the
  # executor retains no state for idle keys.
  #
  # A task which raises an exception is logged and discarded; the exception
  # never prevents later tasks for the same key from running.
  #
  # @example Account updates
  #   executor = Concurrent::KeySerializedExecutor.new(2, max_queue: 100)
  #   # These run one after the other, in order:
  #   executor.post(:account_a) { transfer_money(:account_a) }
  #   executor.post(:account_a) { transfer_money(:account_a) }
  #   # This may run at the same time as the :account_a tasks:
  #   executor.post(:account_b) { transfer_money(:account_b) }
  #
  # @!macro thread_pool_options
  class KeySerializedExecutor < AbstractExecutorService

    # Per-key bookkeeping. Always accessed under the executor's lock.
    #
    # @!visibility private
    KeyState = Struct.new(:queue, :scheduled) do
      def initialize
        super([], false)
      end
    end

    private_constant :KeyState

    # Create a new executor.
    #
    # @param [Integer] num_threads the fixed number of worker threads shared by
    #   all keys; the maximum number of different keys that may run in parallel
    # @param [Hash] opts the options defining executor behavior
    # @option opts [Integer] :max_queue (0) the maximum total number of tasks
    #   which may be waiting for or executing on a worker at any given time,
    #   summed across all keys. A value of zero (the default) means the total
    #   is unbounded.
    # @option opts [Symbol] :fallback_policy (:abort) the policy to apply when
    #   a new task is rejected because the executor is shut down or the total
    #   queue limit has been reached. `:abort` raises `RejectedExecutionError`;
    #   `:discard` silently drops the task and returns `false`; `:caller_runs`
    #   blocks the calling thread until room is available (or the executor
    #   shuts down), providing back pressure while preserving per-key order.
    # @option opts [String] :name the name of the executor, used when naming
    #   its worker threads
    #
    # @raise [ArgumentError] if `num_threads` is less than one
    # @raise [ArgumentError] if `:max_queue` is negative
    # @raise [ArgumentError] if `:fallback_policy` is not a known policy
    def initialize(num_threads, opts = {})
      raise ArgumentError.new('number of threads must be greater than zero') if num_threads.to_i < 1

      @max_queue       = opts.fetch(:max_queue, 0).to_i
      @fallback_policy = opts.fetch(:fallback_policy, :abort)
      raise ArgumentError.new('maximum queue size cannot be negative') if @max_queue < 0
      raise ArgumentError.new("#{@fallback_policy} is not a valid fallback policy") unless FALLBACK_POLICIES.include?(@fallback_policy)

      super(opts.merge(num_threads: num_threads))
    end

    # Submit a task associated with `key` for asynchronous processing.
    #
    # Tasks sharing a key are serialized; tasks with different keys may run in
    # parallel.
    #
    # @param [Object] key the key which the task is serialized on
    # @param [Array] args zero or more arguments to be passed to the task
    #
    # @yield the asynchronous task to perform
    #
    # @return [Boolean] `true` when the task was accepted, otherwise the value
    #   dictated by the fallback policy (`false` when discarded)
    #
    # @raise [RejectedExecutionError] when the executor is shut down or the
    #   total queue limit is reached and the policy is `:abort`
    # @raise [ArgumentError] if no task is given
    def post(key, *args, &task)
      raise ArgumentError.new('no block given') unless block_given?

      deferred_action = synchronize do
        if ns_running? && ns_has_room?
          ns_enqueue(key, args, task)
          nil
        elsif ns_running? && @fallback_policy == :caller_runs
          # Back pressure: sleep on the lock's condition until a completed
          # task frees a slot, then enqueue. Enqueuing only here preserves the
          # per-key submission order and serialization guarantee.
          ns_wait_until { !ns_running? || ns_has_room? }
          if ns_running? && ns_has_room?
            ns_enqueue(key, args, task)
            nil
          else
            fallback_action { task.call(*args) }
          end
        else
          fallback_action { task.call(*args) }
        end
      end

      deferred_action ? deferred_action.call : true
    end

    # Submit a task serialized under the `nil` key.
    #
    # @param [Proc] task the asynchronous task to perform
    # @return [self] the executor itself
    def <<(task)
      post(nil, &task)
      self
    end

    # The executor serializes only within a key, never across different keys.
    #
    # @return [Boolean] always `false`
    def serialized?
      false
    end

    # The total queue is bounded only when a positive `:max_queue` was given.
    #
    # @return [Boolean] `true` when a finite `:max_queue` was configured
    def can_overflow?
      @max_queue > 0
    end

    # @!attribute [r] max_queue
    # @return [Integer] the configured maximum total number of pending (waiting
    #   or running) tasks; zero means unbounded
    attr_reader :max_queue

    # @return [Integer] the number of worker threads shared by all keys
    def length
      @pool.length
    end

    # @return [Integer] the number of accepted tasks not yet completed, summed
    #   across all keys
    def queue_length
      synchronize { @pending }
    end

    # @return [Integer] the number of keys which currently have tasks queued or
    #   running. Keys are reclaimed automatically once idle, so this returns
    #   zero once every accepted task has finished.
    def key_count
      synchronize { @keys.length }
    end

    # @return [Integer] the number of tasks accepted for execution since
    #   construction (including tasks later dismissed by {#kill})
    def scheduled_task_count
      synchronize { @scheduled_task_count }
    end

    # Begin an orderly shutdown: no new tasks are accepted, but every task
    # already in a key's queue is executed.
    #
    # @return [true]
    def shutdown
      action = synchronize do
        break :none unless ns_running?
        @stop_event.set
        ns_broadcast # wake any threads blocked in #post via :caller_runs
        if @pending.zero?
          ns_mark_terminated
          :shutdown_pool
        else
          # The last completing worker shuts the pool down once the accepted
          # queues have drained.
          :none
        end
      end

      @pool.shutdown if action == :shutdown_pool
      true
    end

    # Begin an immediate shutdown: tasks still waiting in key queues are
    # dismissed, in-progress tasks are allowed to finish, and no new tasks are
    # accepted.
    #
    # @return [true]
    def kill
      synchronize do
        break if ns_shutdown?
        @stop_event.set
        ns_broadcast

        # Dismiss every task which has not started running. Workers currently
        # executing a task are allowed to finish that task; when they call
        # #after_task they will find their key queue empty and simply reclaim
        # the key. Run-pending blocks still waiting in the inner pool's queue
        # never started and are dismissed along with it.
        @ready.clear
        @keys.each_value { |state| state.queue.clear }
        @pending = 0
        # Drop all key state. A task in flight on a worker at this moment is
        # still allowed to finish, but its after_task cleanup is a no-op
        # because it no longer appears in a fresh @keys map (so key_count is
        # zero immediately after kill).
        @keys.clear

        ns_mark_terminated
      end
      @pool.kill
      true
    end

    # @!macro executor_service_method_wait_for_termination
    def wait_for_termination(timeout = nil)
      return false unless @stopped_event.wait(timeout)
      @pool.wait_for_termination(timeout)
    end

    private

    def ns_initialize(opts, &block)
      @stop_event    = Event.new
      @stopped_event = Event.new

      @keys    = {}
      @ready   = []
      @pending = 0
      @scheduled_task_count = 0


      pool_opts = {
        max_queue:       0, # never rejects: @keys/@ready provide the real bound
        fallback_policy: :caller_runs,
        auto_terminate:  false
      }
      pool_opts[:name] = opts[:name] if opts.key?(:name)
      @pool = FixedThreadPool.new(opts.fetch(:num_threads), pool_opts)
    end

    def ns_running?
      !@stop_event.set?
    end

    def ns_shutdown?
      @stopped_event.set?
    end

    def ns_shuttingdown?
      @stop_event.set? && !@stopped_event.set?
    end

    def ns_has_room?
      @max_queue.zero? || @pending < @max_queue
    end

    # Append a task to its key's queue and schedule the key if it is idle.
    # Must be called under the lock with room already confirmed.
    def ns_enqueue(key, args, task)
      state = @keys[key] ||= KeyState.new
      state.queue << [task, args]
      @pending += 1
      @scheduled_task_count += 1
      unless state.scheduled
        state.scheduled = true
        @ready << state
        ns_dispatch
      end
    end

    # Hand as many ready keys as possible to worker threads. Called under the
    # lock; @pool.post never blocks because the inner pool has an unbounded
    # queue. The state is passed as a block argument so it is captured now;
    # reading @ready later would find it already sliced away.
    def ns_dispatch
      states = @ready.dup
      @ready.clear
      posted = 0
      while posted < states.length
        break unless @pool.post(states[posted]) { |state| run_pending(state) }
        posted += 1
      end
      # Defensive: posts to the unbounded inner pool do not reject while
      # running, but if any did, return those keys to the head of the line so
      # their FIFO position is not lost.
      @ready = states[posted..-1].concat(@ready) if posted < states.length
    end

    # Runs on a worker thread. Executes exactly one task for the key and, in
    # #after_task, moves the key to the back of the ready line so that other
    # keys are served before it is scheduled again.
    def run_pending(state)
      task, args = synchronize { state.queue.first }
      return if task.nil?

      begin
        task.call(*args)
      rescue => ex
        # A failing task must never stop the key's later tasks from running.
        log DEBUG, ex
      end
    ensure
      after_task(state)
    end

    def after_task(state)
      action = synchronize do
        if state.queue.empty?
          # The executor was killed while this task was in flight. Its queued
          # successors (if any) have already been dismissed, so just reclaim
          # the key and stop.
          @keys.delete_if { |_, existing| existing.equal?(state) }
          :none
        else
          @pending -= 1
          state.queue.shift
          ns_broadcast # a slot in the total queue has been freed

          if state.queue.empty?
            # Reclaim the now-idle key. Identity comparison guards against the
            # window in which a fresh state for the same key was created after
            # this state had already been observed empty.
            @keys.delete_if { |_, existing| existing.equal?(state) }
          else
            # Back of the line: other ready keys are served before this busy
            # key runs again, while its own tasks still execute strictly in
            # order. During an orderly shutdown we keep draining the queue.
            @ready << state
            ns_dispatch
          end

          if @stop_event.set? && @pending.zero?
            ns_mark_terminated
            :shutdown_pool
          else
            :none
          end
        end
      end

      @pool.shutdown if action == :shutdown_pool
    end

    # Mark the executor terminated. The actual pool shut-down is performed by
    # the caller outside the lock (a worker thread may be the caller).
    def ns_mark_terminated
      @stopped_event.set
    end
  end
end
