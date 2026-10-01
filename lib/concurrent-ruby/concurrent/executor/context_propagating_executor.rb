require 'concurrent/context'
require 'concurrent/synchronization/object'
require 'concurrent/executor/executor_service'

module Concurrent
  # A delegating executor which propagates explicitly registered {ContextVar}s
  # from the submitting thread to the thread executing the task.
  #
  # The context is captured when {#post} is called (on the submitting thread)
  # and installed on the executing thread for the duration of the task. When the
  # task finishes - even if it raises - the executing thread's previous bindings
  # are restored. Because the restore also clears bindings that did not exist
  # beforehand, a worker thread reused for another request never observes the
  # previous request's context.
  #
  # Restoration is equally guaranteed when the wrapped executor rejects the task
  # and its `:caller_runs` fallback executes it on the submitting thread, and
  # when the wrapped executor runs tasks inline (such as {ImmediateExecutor}).
  #
  # @example
  #   trace_id = Concurrent::ContextVar.new(:trace_id)
  #   pool     = Concurrent::FixedThreadPool.new(2)
  #   executor = Concurrent::ContextPropagatingExecutor.new(pool)
  #
  #   trace_id.value = 'request-1'
  #   executor.post { puts trace_id.value } #=> "request-1" on a pool worker
  class ContextPropagatingExecutor < Synchronization::Object
    safe_initialization!

    include ExecutorService

    # @param [Executor] executor the executor to delegate the tasks to
    # @param [Hash] opts the options
    # @option opts [Context::Snapshot, nil] :snapshot an optional fixed
    #   snapshot to use for every task. When `nil` (the default) the context is
    #   captured on the submitting thread at every {#post} call.
    def initialize(executor, opts = {})
      super()
      @executor = executor
      @snapshot = opts.fetch(:snapshot, nil)
    end

    # @return [Executor] the wrapped executor
    attr_reader :executor

    # @!macro executor_service_method_post
    #
    # Captures the registered context variables of the calling thread and runs
    # the task with them installed on the executing thread.
    def post(*args, &task)
      raise ArgumentError.new('no block given') unless block_given?

      snapshot = @snapshot || Context.capture
      wrapped  = ->(*task_args) { Context.with_snapshot(snapshot) { task.call(*task_args) } }
      @executor.post(*args, &wrapped)
    end

    # @!macro executor_service_method_left_shift
    def <<(task)
      post(&task)
      self
    end

    # @!macro executor_service_method_can_overflow_question
    def can_overflow?
      @executor.can_overflow?
    end

    # @!macro executor_service_method_serialized_question
    def serialized?
      @executor.serialized?
    end
  end
end
