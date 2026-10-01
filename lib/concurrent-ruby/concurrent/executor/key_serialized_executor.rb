require 'thread'
require 'concurrent/errors'
require 'concurrent/concern/logging'
require 'concurrent/atomic/event'
require 'concurrent/executor/abstract_executor_service'
require 'concurrent/executor/fixed_thread_pool'

module Concurrent

  # An executor which serializes the execution of tasks sharing the same key
  # while running tasks belonging to different keys in parallel.
  #
  # Tasks posted with the same key are guaranteed to:
  # * be executed in the order they were {#post posted}, and
  # * never overlap with each other.
  #
  # Tasks posted with different keys may run in parallel, up to the size of the
  # underlying fixed-size thread pool which is shared between all the keys. A
  # busy key cannot starve other keys: after each task the worker releases the
  # key back to the shared pool and only re-enqueues the follow-up task, so a
  # queued task belonging to another key is always able to make progress.
  #
  # The total number of tasks accepted but not yet completed is bounded by
  # `max_tasks` (independently of the number of distinct keys). When the limit
  # is reached, new tasks are handled according to `fallback_policy`.
  #
  # Keys without pending work are reclaimed automatically: an idle key retains
  # no state and may be garbage collected.
  #
  # @example Account updates
  #   executor = Concurrent::KeySerializedExecutor.new(4)
  #
  #   # updates of the same account never overlap and stay in submission order,
  #   # different accounts are processed in parallel
  #   executor.post(account_id) { account.update!(payload) }
  #
  # @!macro abstract_executor_service_public_api
  class KeySerializedExecutor < AbstractExecutorService
    include Concern::Logging

    # Default maximum number of accepted-but-not-yet-completed tasks.
    DEFAULT_MAX_TASKS = 1_000

    # @!attribute [r] max_threads
    #   @return [Integer] the (fixed) number of worker threads shared by all keys
    attr_reader :max_threads

    # @!attribute [r] max_tasks
    #   @return [Integer] the maximal number of tasks accepted but not yet
    #     completed (across all keys); `0` means unlimited
    attr_reader :max_tasks

    # @param [Integer] num_threads the number of worker threads in the shared
    #   pool; must be greater than zero
    # @param [Hash] opts the options
    # @option opts [Integer] :max_tasks (DEFAULT_MAX_TASKS) the maximal number
    #   of tasks accepted but not yet completed (across all keys); `0` means
    #   unlimited
    # @option opts [Symbol] :fallback_policy (`:abort`) the policy handling a
    #   task which cannot be accepted because the task limit was reached or the
    #   executor is shutting down: `:abort` raises {RejectedExecutionError},
    #   `:discard` rejects the task and returns `false`, `:caller_runs` blocks
    #   the calling thread until the key is free and runs the task there,
    #   preserving per-key ordering
    #
    # @note With the `:caller_runs` policy a saturated {#post} blocks, so tasks
    #   which post other tasks back into a saturated executor must not create a
    #   circular wait: e.g. a running task blocking on another *key* which is
    #   itself blocked can deadlock the small worker pool. A re-entrant post to
    #   the task's *own* key is detected and executed inline.
    # @option opts [Boolean] :auto_terminate (true) whether the pool threads are
    #   daemon threads
    #
    # @raise [ArgumentError] if `num_threads` is not greater than zero,
    #   `max_tasks` is negative or `fallback_policy` is not a known policy
    def initialize(num_threads, opts = {})
      @num_threads = num_threads
      super(opts)
    end

    # Submit a task to the executor for asynchronous processing.
    #
    # @param [Object] key any object used to group tasks; tasks with equal
    #   (`#eql?` / `#hash`) keys are serialized
    # @param [Array] args zero or more arguments to be passed to the task
    #
    # @yield the asynchronous task to perform
    #
    # @return [Boolean] `true` if the task was queued or executed by the
    #   `:caller_runs` policy, `false` if it was discarded (`:discard`) or the
    #   executor was killed while a `:caller_runs` task waited for its key
    #
    # @raise [RejectedExecutionError] if the task cannot be accepted and the
    #   `:abort` policy is in effect
    # @raise [ArgumentError] if no task is given
    def post(key, *args, &task)
      raise ArgumentError.new('no block given') unless block_given?

      deferred_action = synchronize do
        if ns_running? && ns_accept?
          ns_enqueue(key, Job.new(args, task))
          @inflight += 1
          nil
        elsif ns_running? && @fallback_policy == :caller_runs
          # Re-entrant post for a key currently executed by this very thread:
          # blocking would self-deadlock, so run it inline right away. This
          # keeps the per-key ordering guarantee.
          state = @keys[key]
          if state && state[:running].equal?(Thread.current)
            -> { ns_run_inline(args, task) }
          else
            # Capacity reached: a handoff ticket keeps the task ordered with
            # respect to its key while the calling thread executes it.
            handoff = Handoff.new
            ns_enqueue(key, handoff)
            @inflight += 1
            -> { handoff.run(args, task) }
          end
        else
          ns_rejected_action(args, task)
        end
      end

      if deferred_action
        deferred_action.call
      else
        true
      end
    end

    # @!macro executor_service_method_can_overflow_question
    #
    # @return [Boolean] `true` when the accepted-task queue is bounded,
    #   i.e. `max_tasks` is not unlimited
    def can_overflow?
      synchronize { @max_tasks != 0 }
    end

    # @return [Integer] the number of tasks accepted but not yet completed,
    #   across all keys
    def scheduled_task_count
      synchronize { @inflight }
    end

    # @return [Integer] the number of keys which currently have pending work
    def key_count
      synchronize { @keys.size }
    end

    # @!macro executor_service_method_shutdown
    #
    # No new tasks are accepted; all already accepted tasks are still executed
    # in per-key submission order before the executor terminates.
    def shutdown
      synchronize do
        break unless ns_running?
        @StopEvent.set
        if @inflight.zero?
          @pool.shutdown
          @StoppedEvent.set
        end
      end
      true
    end

    # @!macro executor_service_method_kill
    #
    # In-progress tasks finish but all other accepted tasks are dismissed and
    # threads blocked in a `:caller_runs` {#post} are woken up immediately.
    def kill
      synchronize do
        break if ns_shutdown?
        @StopEvent.set
        @keys.each_value do |state|
          state[:queue].each { |ticket| ticket.cancel if ticket.is_a?(Handoff) }
        end
        @keys.clear
        @pool.kill
        @StoppedEvent.set
      end
      true
    end

    # @!macro executor_service_method_wait_for_termination
    def wait_for_termination(timeout = nil)
      @StoppedEvent.wait(timeout)
    end

    private

    # A blocking hand-off channel used by the :caller_runs policy. A worker
    # drains the ticket (waits for the caller to publish the task), the caller
    # executes the task and then releases the worker to continue the key.
    #
    # @!visibility private
    class Handoff
      def initialize
        @mutex     = Mutex.new
        @cond      = ConditionVariable.new
        @args      = nil
        @task      = nil
        @published = false # caller published the task
        @granted   = false # worker granted the key's turn
        @finished  = false # caller finished executing the task
        @cancelled = false
      end

      # Called by the submitting thread: publish the task, wait for the key's
      # turn, execute the task (on this thread) and release the worker.
      #
      # @return [Boolean] `true` if the task ran, `false` if the executor was
      #   killed while waiting
      def run(args, task)
        @mutex.synchronize do
          return false if @cancelled
          @args, @task = args, task
          @published = true
          @cond.broadcast
          @cond.wait(@mutex) until @granted || @cancelled
          return false if @cancelled
        end

        begin
          task.call(*args)
        ensure
          @mutex.synchronize do
            @finished = true
            @cond.broadcast
          end
        end
        true
      end

      # Called by a worker: wait for the caller to publish the task, grant the
      # key's turn and wait until the task finished.
      #
      # @return [Boolean] `false` when cancelled before the task completed
      def take
        @mutex.synchronize do
          @cond.wait(@mutex) until @published || @cancelled
          return false if @cancelled
          @granted = true
          @cond.broadcast
          @cond.wait(@mutex) until @finished || @cancelled
          !@cancelled
        end
      end

      # Called by #kill to unblock caller and worker.
      def cancel
        @mutex.synchronize do
          @cancelled = true
          @cond.broadcast
        end
      end
    end
    private_constant :Handoff

    Job = Struct.new(:args, :block) do
      def call
        block.call(*args)
      end
    end
    private_constant :Job

    def ns_initialize(opts)
      @max_threads     = @num_threads.to_i
      @max_tasks       = opts.fetch(:max_tasks, DEFAULT_MAX_TASKS).to_i
      @fallback_policy = opts.fetch(:fallback_policy, :abort)

      raise ArgumentError.new('number of threads must be greater than zero') if @max_threads < 1
      raise ArgumentError.new('max_tasks cannot be negative') if @max_tasks < 0
      unless FALLBACK_POLICIES.include?(@fallback_policy)
        raise ArgumentError.new("#{@fallback_policy} is not a valid fallback policy")
      end

      # key => { queue: Array<Job|Handoff>, active: Boolean }
      @keys     = {}
      @inflight = 0

      @StopEvent    = Event.new
      @StoppedEvent = Event.new

      pool_opts = { auto_terminate: @auto_terminate }
      pool_opts[:name] = @name if @name
      @pool = FixedThreadPool.new(@max_threads, pool_opts)
    end

    def ns_accept?
      @max_tasks == 0 || @inflight < @max_tasks
    end

    # Appends a ticket to the key's queue and starts a drain if the key was
    # idle. Must be called under the executor lock.
    def ns_enqueue(key, ticket)
      state = @keys[key] ||= { queue: [], active: false }
      state[:queue] << ticket
      ns_schedule(state) unless state[:active]
    end

    # Hands the key to the shared pool. A drain executes exactly one ticket and
    # reschedules itself, releasing the worker in between. Must be called under
    # the executor lock.
    def ns_schedule(state)
      posted = @pool.post { ns_drain(state) }
      state[:active] = true if posted
      posted
    end

    # Pool worker loop body: process one ticket of one key, then release the
    # worker and re-enqueue the follow-up ticket.
    def ns_drain(state)
      ticket = synchronize do
        ticket = state[:queue].first
        state[:running] = Thread.current if ticket
        ticket
      end

      ran = ns_run_ticket(ticket)
      synchronize { state[:running] = nil } if ticket

      reschedule_inline = ns_advance(state, ran)

      # The fixed pool is still running here, so this only guards the
      # unexpected: keep the key chain going on the current worker.
      ns_drain_inline(state) if reschedule_inline
    end

    # Defensive continuation used if the shared pool rejected a follow-up.
    def ns_drain_inline(state)
      loop do
        ticket = synchronize { state[:queue].first }
        break unless ticket

        synchronize { state[:running] = Thread.current }
        ran = ns_run_ticket(ticket)
        synchronize { state[:running] = nil }

        again = ns_advance(state, ran)
        break unless again
      end
    end

    # Marks the head ticket consumed and either re-enqueues the key or reclaims
    # it. Must be called by the worker which just ran the ticket.
    #
    # @return [Boolean] `true` when the pool rejected the follow-up drain and
    #   the caller should continue the chain on the current worker
    def ns_advance(state, ran)
      synchronize do
        if ran
          state[:queue].shift
          @inflight -= 1
        end

        if ns_shutdown?
          # killed: remaining queued tickets are dismissed
          false
        elsif state[:queue].empty?
          state[:active] = false
          @keys.delete_if { |_, other| other.equal?(state) }
          if !ns_running? && @inflight.zero?
            # last accepted task completed after an orderly shutdown
            @pool.shutdown
            @StoppedEvent.set
          end
          false
        else
          !ns_schedule(state)
        end
      end
    end

    # Executes one ticket. Returns false for an empty slot or a cancelled
    # hand-off, true when a real task completed.
    def ns_run_ticket(ticket)
      return false unless ticket

      if ticket.is_a?(Handoff)
        ticket.take
      else
        begin
          ticket.call
        rescue => ex
          # A failing task must not stop the following tasks of the same key.
          log DEBUG, ex
        end
        true
      end
    end

    # Runs a re-entrant task on the calling worker thread, keeping the per-key
    # ordering. Returned as a deferred action by #post (executed unlocked).
    def ns_run_inline(args, task)
      begin
        task.call(*args)
      rescue => ex
        # let it fail
        log DEBUG, ex
      end
      true
    end

    # Returns an action (evaluated by #post outside of the lock) for a task
    # which cannot be accepted.
    def ns_rejected_action(args, task)
      case @fallback_policy
      when :abort
        -> { raise RejectedExecutionError }
      when :discard
        -> { false }
      when :caller_runs
        lambda {
          begin
            task.call(*args)
          rescue => ex
            # let it fail
            log DEBUG, ex
          end
          true
        }
      else
        raise "Unknown fallback policy #{@fallback_policy}"
      end
    end

    def ns_running?
      !@StopEvent.set?
    end

    def ns_shuttingdown?
      @StopEvent.set? && !@StoppedEvent.set?
    end

    def ns_shutdown?
      @StoppedEvent.set?
    end
  end
end
