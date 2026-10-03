require 'thread'
require 'set'
require 'concurrent/errors'
require 'concurrent/utility/monotonic_time'
require 'concurrent/utility/processor_counter'

module Concurrent

  # A fork/join executor with a fixed, hard-capped set of worker threads.
  #
  # Tasks are submitted with {#fork}, which returns a {ForkJoinTask} handle.
  # Joining a handle waits until the task completes and then returns its
  # value (or re-raises its exception). The two kinds of joiners behave
  # differently:
  #
  # * **Worker threads** of this executor *help* while joining: instead of
  #   blocking, a worker whose joined task has not started yet executes it
  #   inline, and otherwise runs other runnable tasks until the joined task
  #   completes. This keeps a fully-occupied pool making progress instead of
  #   deadlocking the way a plain fixed thread pool would.
  # * **External threads** simply wait; they never execute tasks.
  #
  # The number of worker threads is fixed at construction time and never
  # grows; no temporary threads are spawned to work around blockage.
  #
  # Every task body is executed exactly once: a task is claimed atomically,
  # so a task can never run both as "help" inside another thread's join and
  # through a normal queue dequeue.
  #
  # Mutual waits between worker tasks are detected as dependency cycles and
  # broken by raising {ForkJoinExecutor::DependencyCycleError} in the joiner
  # that would close the cycle.
  #
  # {#shutdown} gives every unfinished join a definite outcome: all
  # incomplete tasks are completed with {ForkJoinExecutor::ShutdownError},
  # which is then raised in every current and future joiner of those tasks.
  #
  # Minimal example (single worker, recursive divide and conquer):
  #
  #   pool = Concurrent::ForkJoinExecutor.new(1)
  #   data = (1..128).to_a
  #   sum = nil
  #   sum = lambda do |lo, hi|
  #     if hi - lo <= 8
  #       data[lo...hi].reduce(0, :+)
  #     else
  #       mid = (lo + hi) / 2
  #       left  = pool.fork(lo, mid, &sum)
  #       right = pool.fork(mid, hi, &sum)
  #       left.join + right.join
  #     end
  #   end
  #   pool.fork(0, data.size, &sum).join # => 8256
  #   pool.shutdown
  #
  class ForkJoinExecutor

    # Raised when a join would close a dependency cycle of tasks mutually
    # waiting on each other.
    DependencyCycleError = Class.new(Concurrent::Error)

    # Raised in joiners of tasks that were still incomplete when the
    # executor was shut down.
    ShutdownError = Class.new(Concurrent::Error)

    # @!visibility private
    WORKER_CONTEXT_KEY = :concurrent_fork_join_worker_context

    # A handle to a forked task. The task body runs exactly once; the
    # handle resolves to either its return value or its exception.
    class ForkJoinTask

      # @!visibility private
      def initialize(executor, args, block)
        @executor  = executor
        @args      = args
        @block     = block
        @mutex     = Mutex.new
        @condition = ConditionVariable.new
        @state     = :pending # :pending -> :running -> :done
        @result    = nil
        @exception = nil
        @runner    = nil
      end

      # @return [Boolean] true if the task has completed, successfully or not
      def done?
        @mutex.synchronize { @state == :done }
      end

      # Wait for the task to complete and return its value, re-raising its
      # exception if it failed. Worker threads of the owning executor help
      # execute runnable tasks while waiting; all other threads simply wait.
      #
      # @param [Numeric, nil] timeout seconds to wait, nil waits forever
      # @return [Object] the task's return value
      # @raise [Concurrent::TimeoutError] if the timeout expires
      def join(timeout = nil)
        @executor.send(:join_task, self, timeout)
      end

      # @!visibility private
      def claim(runner)
        @mutex.synchronize do
          if @state == :pending
            @state = :running
            @runner = runner
            true
          else
            false
          end
        end
      end

      # @!visibility private
      def execute
        begin
          value = @block.call(*@args)
          finish(value, nil)
        rescue Exception => error # joiners must always be released
          finish(nil, error)
        end
        nil
      end

      # @!visibility private
      def running_runner
        @mutex.synchronize { @state == :running ? @runner : nil }
      end

      # @!visibility private
      def wait_for_completion(timeout)
        @mutex.synchronize do
          deadline = timeout && Concurrent.monotonic_time + timeout
          until @state == :done
            remaining = deadline && deadline - Concurrent.monotonic_time
            return false if remaining && remaining <= 0
            @condition.wait(@mutex, remaining)
          end
        end
        true
      end

      # @!visibility private
      def result_or_raise
        @mutex.synchronize do
          raise @exception if @exception
          @result
        end
      end

      # @!visibility private
      def fail_unless_done(error)
        finished = @mutex.synchronize do
          if @state == :done
            false
          else
            @state = :done
            @exception = error
            @runner = nil
            @condition.broadcast
            true
          end
        end
        @executor.send(:task_finished, self) if finished
        finished
      end

      private

      def finish(value, exception)
        finished = @mutex.synchronize do
          if @state == :done
            false
          else
            @state = :done
            @result = value
            @exception = exception
            @runner = nil
            @condition.broadcast
            true
          end
        end
        @executor.send(:task_finished, self) if finished
      end
    end

    # @return [Integer] the fixed number of worker threads
    attr_reader :size

    # @param [Integer] size number of worker threads, fixed for the
    #   lifetime of the executor
    def initialize(size = Concurrent.processor_count)
      unless size.is_a?(Integer) && size > 0
        raise ArgumentError, 'size must be a positive Integer'
      end
      @size = size
      @work_mutex = Mutex.new
      @work_condition = ConditionVariable.new
      @queues = Array.new(size) { [] }
      @inbound = []
      @registry = Set.new
      @shutdown = false
      @completed_task_count = 0
      @wait_mutex = Mutex.new
      @waits_for = {}
      @in_join_counts = Hash.new(0)
      @workers = Array.new(size) do |index|
        Thread.new do
          Thread.current.thread_variable_set(WORKER_CONTEXT_KEY, [self, index])
          worker_main(index)
        end
      end
    end

    # Fork a task. When called from a worker of this executor the task is
    # pushed onto that worker's own deque, otherwise it goes to the inbound
    # submission queue.
    #
    # @param [Array] args arguments passed to the task body
    # @yield the task body
    # @return [ForkJoinTask] handle to join on
    # @raise [Concurrent::RejectedExecutionError] if the executor is shut down
    def fork(*args, &block)
      raise ArgumentError, 'no block given' unless block
      task = ForkJoinTask.new(self, args, block)
      @work_mutex.synchronize do
        raise RejectedExecutionError, 'executor has been shut down' if @shutdown
        @registry.add(task)
        index = current_worker_index
        if index
          @queues[index].push(task)
        else
          @inbound.push(task)
        end
        @work_condition.broadcast
      end
      task
    end

    # @!macro executor_service_method_post
    def post(*args, &task)
      raise ArgumentError, 'no block given' unless task
      fork(*args, &task)
      true
    rescue RejectedExecutionError
      false
    end

    # @!macro executor_service_method_can_overflow_question
    def can_overflow?
      false
    end

    # @!macro executor_service_method_serialized_question
    def serialized?
      false
    end

    # @return [Boolean] true if the executor has been shut down
    def shutdown?
      @work_mutex.synchronize { @shutdown }
    end

    # @return [Boolean] true if the executor is accepting and running tasks
    def running?
      !shutdown?
    end

    # @return [Integer] number of tasks submitted but not yet completed
    def scheduled_task_count
      @work_mutex.synchronize { @registry.size }
    end

    # @return [Integer] number of tasks completed so far
    def completed_task_count
      @work_mutex.synchronize { @completed_task_count }
    end

    # @return [Integer] number of worker threads currently inside a join
    def joining_worker_count
      @wait_mutex.synchronize { @in_join_counts.count { |_, count| count > 0 } }
    end

    # Shut the executor down. Every incomplete task is completed with
    # {ShutdownError}, releasing all current and future joiners; no new
    # tasks are accepted. Tasks currently executing finish their bodies but
    # their results are discarded.
    def shutdown
      unfinished = @work_mutex.synchronize do
        if @shutdown
          nil
        else
          @shutdown = true
          @inbound.clear
          @queues.each(&:clear)
          @registry.to_a
        end
      end
      return unless unfinished
      error = ShutdownError.new('fork/join executor has been shut down')
      unfinished.each { |task| task.fail_unless_done(error) }
      @work_mutex.synchronize { @work_condition.broadcast }
      nil
    end

    # Wait for all worker threads to terminate.
    #
    # @param [Numeric, nil] timeout seconds to wait, nil waits forever
    # @return [Boolean] true if all workers terminated in time
    def wait_for_termination(timeout = nil)
      deadline = timeout && Concurrent.monotonic_time + timeout
      @workers.all? do |worker|
        remaining = deadline && deadline - Concurrent.monotonic_time
        if remaining && remaining <= 0
          false
        else
          worker.join(remaining)
          !worker.alive?
        end
      end
    end

    # Shut down and kill the worker threads.
    def kill
      shutdown
      @workers.each(&:kill)
      true
    end

    private

    def current_worker_index
      context = Thread.current.thread_variable_get(WORKER_CONTEXT_KEY)
      context && context[0].equal?(self) ? context[1] : nil
    end

    def join_task(task, timeout)
      index = current_worker_index
      if index
        join_as_worker(task, index, timeout)
      else
        join_as_external(task, timeout)
      end
    end

    def join_as_external(task, timeout)
      unless task.wait_for_completion(timeout)
        raise Concurrent::TimeoutError, 'join timed out'
      end
      task.result_or_raise
    end

    def join_as_worker(task, index, timeout)
      return task.result_or_raise if task.done?
      note_join_entry(index)
      edge_pushed = false
      begin
        # Prefer to just run the joined task ourselves.
        if task.claim(index)
          task.execute
          return task.result_or_raise
        end
        push_wait_edge(index, task) # raises DependencyCycleError on cycles
        edge_pushed = true
        deadline = timeout && Concurrent.monotonic_time + timeout
        loop do
          return task.result_or_raise if task.done?
          helped = nil
          @work_mutex.synchronize do
            unless task.done?
              helped = dequeue_locked(index)
              unless helped
                remaining = deadline && deadline - Concurrent.monotonic_time
                if remaining && remaining <= 0
                  helped = :timeout
                else
                  @work_condition.wait(@work_mutex, remaining)
                end
              end
            end
          end
          raise Concurrent::TimeoutError, 'join timed out' if helped == :timeout
          helped.execute if helped && helped != :timeout && helped.claim(index)
        end
      ensure
        pop_wait_edge(index) if edge_pushed
        note_join_exit(index)
      end
    end

    def worker_main(index)
      loop do
        task = @work_mutex.synchronize do
          dequeued = dequeue_locked(index)
          while dequeued.nil? && !@shutdown
            @work_condition.wait(@work_mutex)
            dequeued = dequeue_locked(index)
          end
          dequeued
        end
        break unless task
        task.execute if task.claim(index)
      end
    end

    # Takes a task from the executor's own deque first (LIFO), then from the
    # inbound submission queue, then steals from other workers (FIFO).
    def dequeue_locked(index)
      own = @queues[index]
      return own.pop unless own.empty?
      return @inbound.shift unless @inbound.empty?
      @queues.each_with_index do |queue, other|
        next if other == index || queue.empty?
        return queue.shift
      end
      nil
    end

    def task_finished(task)
      @work_mutex.synchronize do
        @registry.delete(task)
        @completed_task_count += 1
        @work_condition.broadcast
      end
    end

    def push_wait_edge(index, task)
      @wait_mutex.synchronize do
        stack = (@waits_for[index] ||= [])
        stack.push(task)
        begin
          detect_cycle_locked(index)
        rescue DependencyCycleError
          stack.pop
          @waits_for.delete(index) if stack.empty?
          raise
        end
      end
    end

    def pop_wait_edge(index)
      @wait_mutex.synchronize do
        stack = @waits_for[index]
        if stack
          stack.pop
          @waits_for.delete(index) if stack.empty?
        end
      end
    end

    # Walks the wait-for graph: a worker depends on the workers running the
    # tasks it waits for. Raises if the walk returns to the starting worker.
    def detect_cycle_locked(start)
      visited = { start => true }
      worklist = [start]
      until worklist.empty?
        worker = worklist.pop
        Array(@waits_for[worker]).each do |task|
          runner = task.running_runner
          next if runner.nil?
          if runner == start
            raise DependencyCycleError,
                  'join would close a dependency cycle between tasks'
          end
          unless visited[runner]
            visited[runner] = true
            worklist.push(runner)
          end
        end
      end
    end

    def note_join_entry(index)
      @wait_mutex.synchronize { @in_join_counts[index] += 1 }
    end

    def note_join_exit(index)
      @wait_mutex.synchronize do
        @in_join_counts[index] -= 1
        @in_join_counts.delete(index) if @in_join_counts[index] <= 0
      end
    end
  end
end
