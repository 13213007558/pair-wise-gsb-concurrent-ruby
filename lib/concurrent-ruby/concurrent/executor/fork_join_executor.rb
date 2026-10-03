require 'concurrent/errors'
require 'concurrent/utility/monotonic_time'
require 'concurrent/utility/processor_counter'

module Concurrent

  # A fork/join pool with a hard upper bound on the number of worker threads.
  #
  # Unlike a plain fixed thread pool, a worker that joins a task forked into
  # the same pool does not simply block: while its own join target is
  # unfinished it *helps* the pool by executing other runnable tasks. This
  # allows recursive divide-and-conquer algorithms to run on a bounded --
  # even single-threaded -- pool without starving, something a naive
  # thread pool deadlocks on as soon as every worker waits for a queued
  # subtask.
  #
  # Joins performed by threads that do not belong to the pool (external
  # callers) simply wait for the task to complete.
  #
  # Additional guarantees:
  #
  # * The worker count never exceeds +max_threads+; no overflow or
  #   compensating threads are ever created.
  # * Every forked task is executed exactly once: a task is claimed either
  #   by a helping worker or by a worker dequeuing it, never both.
  # * Tasks may fork nested subtasks and join any number of results.
  # * Exceptions raised by a task are re-raised by its +join+.
  # * Workers mutually waiting on each other's tasks form a dependency
  #   cycle; the cycle is detected and broken by raising
  #   {CyclicDependencyError} instead of deadlocking the pool.
  # * {#shutdown} fails every still-queued task with {ShutdownError}, so
  #   joins of tasks that will never run get a definite outcome.
  #
  # @example Recursive parallel sum on a single worker
  #   pool = Concurrent::ForkJoinExecutor.new(max_threads: 1)
  #   sum = nil
  #   sum = ->(lo, hi) do
  #     if hi - lo <= 10
  #       (lo...hi).sum
  #     else
  #       mid = (lo + hi) / 2
  #       left  = pool.fork { sum.call(lo, mid) }
  #       right = pool.fork { sum.call(mid, hi) }
  #       left.join + right.join
  #     end
  #   end
  #   pool.fork { sum.call(0, 100) }.join # => 4950
  #   pool.shutdown
  class ForkJoinExecutor

    # Raised when workers wait on each other's tasks in a way that can
    # never make progress (a wait-for cycle, including a task joining
    # itself).
    CyclicDependencyError = Class.new(Error)

    # Raised by +join+ when the pool was shut down before the task could
    # run. Gives unfinished joins a definite outcome.
    ShutdownError = Class.new(Error)

    # A handle to a task forked into a {ForkJoinExecutor}, returned by
    # {ForkJoinExecutor#fork}.
    class ForkJoinTask

      # Blocks until the task completes and returns its value. If the task
      # raised an exception, that same exception is re-raised here.
      #
      # When called from a worker thread of the owning pool, the calling
      # worker helps execute runnable tasks while waiting.
      #
      # @return [Object] the task's result
      def join
        @pool.send :join_task, self
      end

      # @return [Boolean] true when the task has finished (normally or
      #   with an exception)
      def done?
        @pool.send :task_done?, self
      end

      # Blocks like {#join} but returns +nil+ instead of re-raising when
      # the task failed.
      #
      # @return [Object, nil] the task's result, or nil on failure
      def value
        join
      rescue StandardError
        nil
      end

      # @return [Exception, nil] the exception the task failed with,
      #   or nil if it has not failed (or has not finished)
      def reason
        @pool.send :task_reason, self
      end

      private

      def initialize(pool, block)
        @pool    = pool
        @block   = block
        @state   = :pending # :pending -> :running -> :done
        @value   = nil
        @error   = nil
        @runner  = nil # worker executing the task while :running
      end

      # All methods below are internal state transitions invoked by the
      # pool; the state methods are only called while holding the pool's
      # lock, except #run which executes user code outside of it.

      def pending?
        @state == :pending
      end

      def running?
        @state == :running
      end

      def finished?
        @state == :done
      end

      def runner
        @runner
      end

      def claim(runner)
        @state  = :running
        @runner = runner
      end

      def run
        @value = @block.call
      rescue Exception => e # rubocop:disable Lint/RescueException
        @error = e
      end

      def finish
        @state  = :done
        @runner = nil
      end

      def fail_with(error)
        @error = error
        @state = :done
      end

      def outcome
        raise @error if @error
        @value
      end

      def failure
        @error
      end
    end

    # @return [Integer] the hard upper bound of worker threads
    attr_reader :max_threads

    # @param [Integer] max_threads hard upper bound of worker threads;
    #   the pool never creates more threads than this
    def initialize(max_threads: [Concurrent.processor_count, 2].max)
      raise ArgumentError, 'max_threads must be a positive Integer' unless max_threads.is_a?(Integer) && max_threads > 0

      @max_threads      = max_threads
      @mutex            = Mutex.new
      @condition        = ConditionVariable.new
      @queue            = [] # pending ForkJoinTasks, FIFO
      @shutdown         = false
      @workers          = []
      @worker_by_thread = {}

      max_threads.times { |id| spawn_worker id }
    end

    # Forks a new task into the pool.
    #
    # @return [ForkJoinTask] handle used to join the task's result
    # @raise [RejectedExecutionError] if the pool has been shut down
    def fork(&block)
      raise ArgumentError, 'no block given' unless block

      task = ForkJoinTask.new self, block
      @mutex.synchronize do
        raise RejectedExecutionError, 'fork/join pool is shut down' if @shutdown
        @queue.push task
        @condition.signal
      end
      task
    end

    # Stops the pool from accepting new tasks and fails every task still
    # waiting in the queue with {ShutdownError}, giving joins of those
    # tasks a definite outcome. Tasks already running are left to finish.
    #
    # @return [true]
    def shutdown
      @mutex.synchronize do
        unless @shutdown
          @shutdown = true
          until @queue.empty?
            @queue.shift.send :fail_with, ShutdownError.new('pool shut down before the task could run')
          end
          @condition.broadcast
        end
      end
      true
    end

    # @return [Boolean] true after {#shutdown} has been called
    def shutdown?
      @mutex.synchronize { @shutdown }
    end

    # Blocks until all worker threads have exited (after {#shutdown}),
    # or until the timeout expires.
    #
    # @param [Numeric, nil] timeout maximum number of seconds to wait;
    #   nil waits indefinitely
    # @return [Boolean] true if the pool terminated
    def wait_for_termination(timeout = nil)
      deadline = timeout && Concurrent.monotonic_time + timeout
      @workers.all? do |worker|
        remaining = deadline ? deadline - Concurrent.monotonic_time : nil
        next false if remaining && remaining <= 0
        worker[:thread].join(remaining)
        !worker[:thread].alive?
      end
    end

    private

    def spawn_worker(id)
      worker = { id: id, thread: nil, waiting_on: nil }
      thread = Thread.new { worker_loop worker }
      thread.name = "fork-join-executor-worker-#{id}" if thread.respond_to? :name=
      worker[:thread] = thread
      @workers << worker
      @worker_by_thread[thread] = worker
    end

    def worker_loop(worker)
      loop do
        task = @mutex.synchronize do
          @condition.wait @mutex while @queue.empty? && !@shutdown
          break if @queue.empty? # shut down and drained
          claim_locked @queue.shift, worker
        end
        break unless task
        run_task task
      end
    end

    # Transitions the task from :pending to :running. Called with the
    # lock held; together with removing the task from the queue this
    # guarantees exactly one executor per task.
    def claim_locked(task, worker)
      task.send :claim, worker
      task
    end

    def run_task(task)
      task.send :run
      @mutex.synchronize do
        task.send :finish
        @condition.broadcast
      end
    end

    def task_done?(task)
      @mutex.synchronize { task.send(:finished?) }
    end

    def task_reason(task)
      @mutex.synchronize { task.send(:failure) }
    end

    def join_task(task)
      worker = @worker_by_thread[Thread.current]
      worker ? help_while_joining(worker, task) : await_task(task)
      @mutex.synchronize { task.send :outcome }
    end

    # External callers simply wait for completion.
    def await_task(task)
      @mutex.synchronize do
        @condition.wait @mutex until task.send(:finished?)
      end
    end

    # A worker joining a task of its own pool helps execute runnable
    # tasks until its join target completes.
    def help_while_joining(worker, target)
      until task_done? target
        claimed = @mutex.synchronize do
          next if target.send :finished?

          runnable =
            if target.send(:pending?) && @queue.delete(target)
              target
            elsif !@queue.empty?
              @queue.shift
            end

          if runnable
            claim_locked runnable, worker
          else
            # Nothing runnable: the target is being executed by another
            # worker. Wait for it, but refuse to close a wait-for cycle.
            worker[:waiting_on] = target
            begin
              check_cycle_locked worker
              @condition.wait @mutex unless target.send(:finished?)
            ensure
              worker[:waiting_on] = nil
            end
            nil
          end
        end
        run_task claimed if claimed
      end
    end

    # Follows the wait-for chain (worker -> task it waits on -> worker
    # running that task -> ...) and raises if it loops back to the given
    # worker. Called with the lock held.
    def check_cycle_locked(worker)
      visited = {}
      current = worker[:waiting_on]
      while current && current.send(:running?) && current.send(:runner)
        runner = current.send :runner
        if runner.equal? worker
          raise CyclicDependencyError, 'workers are waiting on each other in a cycle'
        end
        break if visited[runner]
        visited[runner] = true
        current = runner[:waiting_on]
      end
    end
  end
end
