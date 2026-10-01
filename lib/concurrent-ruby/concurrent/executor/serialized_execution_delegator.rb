require 'delegate'
require 'concurrent/context'
require 'concurrent/executor/serial_executor_service'
require 'concurrent/executor/serialized_execution'

module Concurrent

  # A wrapper/delegator for any `ExecutorService` that
  # guarantees serialized execution of tasks.
  #
  # @see [SimpleDelegator](http://www.ruby-doc.org/stdlib-2.1.2/libdoc/delegate/rdoc/SimpleDelegator.html)
  # @see Concurrent::SerializedExecution
  class SerializedExecutionDelegator < SimpleDelegator
    include SerialExecutorService

    def initialize(executor)
      @executor   = executor
      @serializer = SerializedExecution.new
      super(executor)
    end

    # @!macro executor_service_method_post
    def post(*args, &task)
      raise ArgumentError.new('no block given') unless block_given?
      return false unless running?
      # Capture at submission time: stashed jobs run later on another thread
      # where the submitting context would otherwise be unavailable.
      @serializer.post(@executor, *args, &Context.propagate(task))
    end
  end
end
