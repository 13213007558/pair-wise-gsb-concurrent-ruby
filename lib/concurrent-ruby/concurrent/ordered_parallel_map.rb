require 'concurrent/errors'
require 'concurrent/configuration'
require 'concurrent/synchronization/object'
require 'concurrent/synchronization/lock'

module Concurrent

  # Maps the elements of an enumerable in parallel on an executor, while
  # delivering the mapped values to the consumer strictly in input order —
  # exactly like a serial `Enumerable#map`, but with many elements being
  # processed at once.
  #
  # It is designed for very long, one-shot inputs (e.g. an `Enumerator`
  # reading a stream): the input is consumed lazily, one element at a time,
  # it is never converted to an array and it is never rewound.
  #
  # The number of elements which are either *being processed* or *already
  # processed but not yet delivered* (because an earlier element is still
  # pending) never exceeds the configured `window`. Memory usage therefore
  # stays bounded no matter how long the input is or how unevenly the
  # individual tasks complete.
  #
  # ## Termination behavior
  #
  # *   When the *input* raises while being pulled, the error is re-raised
  #     to the consumer and no further elements are pulled.
  # *   When a *task* raises, the error is re-raised to the consumer at the
  #     position of the failed element — earlier results are delivered
  #     first, in order — and no further elements are pulled.
  # *   When the *consumer* stops early (e.g. `break`, or an exception
  #     raised from its block) no further elements are pulled and buffered
  #     results are released. Use the block form of `Stream#each` (or call
  #     `Stream#close`) to get this guarantee deterministically.
  #
  # In all cases tasks already running on the executor are left to finish
  # but their results are discarded; the executor itself is not shut down.
  #
  # @example
  #   mapper = Concurrent::OrderedParallelMap.new(window: 8, executor: :io)
  #   mapper.map(huge_enumerator) { |item| expensive_fetch(item) }.each do |result|
  #     store(result)
  #   end
  #
  class OrderedParallelMap

    # Default maximum number of elements in flight (being processed or
    # processed but not yet delivered).
    DEFAULT_WINDOW = 10

    # @return [Integer] maximum number of elements in flight at any moment
    attr_reader :window

    # @return [ExecutorService] executor the mapping tasks are posted to
    attr_reader :executor

    # @param [Integer] window maximum number of elements in flight
    #   (running + completed but not yet delivered) at any moment;
    #   must be >= 1
    # @param [Symbol, ExecutorService] executor the executor to run the
    #   mapping tasks on; a symbol (`:io`, `:fast`, `:immediate`) selects
    #   one of the global executors, an {ExecutorService} instance is used
    #   directly
    def initialize(window: DEFAULT_WINDOW, executor: :io)
      unless window.is_a?(Integer) && window >= 1
        raise ArgumentError, "window must be an Integer >= 1, got #{window.inspect}"
      end
      @window   = window
      @executor = Concurrent.executor(executor)
    end

    # Builds a lazy, one-shot stream of mapped values. Nothing is pulled
    # from the input and no task is posted until the returned stream is
    # iterated.
    #
    # @param [#each] input the elements to map; consumed lazily through
    #   `#each`/`#next`, never `#to_a`, never `#rewind`
    # @param task [Proc] maps one element to its result; may be executed
    #   concurrently on the executor, so it must be thread-safe
    # @return [Stream] an `Enumerable` yielding the mapped values in input
    #   order; it can be iterated only once
    def map(input, &task)
      raise ArgumentError, 'OrderedParallelMap#map requires a block' unless task
      unless input.respond_to?(:each)
        raise ArgumentError, "input must respond to #each, got #{input.inspect}"
      end
      Stream.new(input, task, @window, @executor)
    end

    # A lazy, one-shot, ordered view over the mapped elements. Includes
    # {Enumerable}.
    #
    # Iterating with a block (`#each` and all `Enumerable` methods built on
    # it) guarantees that the stream is terminated and its buffered results
    # released when the iteration ends — normally, with an error, or by
    # stopping early. When the stream is iterated through the
    # `Enumerator` returned by the no-block form of `#each`, early
    # abandonment cannot be detected reliably; call {#close} explicitly in
    # that case.
    class Stream < Synchronization::Object
      include Enumerable

      safe_initialization!

      # @!visibility private
      Completed = Struct.new(:value)
      # @!visibility private
      Failed    = Struct.new(:error)
      private_constant :Completed, :Failed

      def initialize(input, task, window, executor) # :nodoc:
        super()
        @Items    = input.each # external iterator, pulled one element at a time
        @Task     = task
        @Window   = window
        @Executor = executor
        @Lock     = Synchronization::Lock.new
        @Results  = {}
        @Started  = false
        @Aborted  = false
      end

      # Yields the mapped values in input order. The stream can be consumed
      # only once; a second iteration raises {LifecycleError}.
      #
      # @return [Enumerator] when no block is given
      # @raise [LifecycleError] when the stream was already consumed or
      #   closed while iterating
      def each
        return enum_for(:each) unless block_given?

        @Lock.synchronize do
          raise LifecycleError, 'this stream has already been consumed' if @Started
          @Started = true
        end

        begin
          drain { |value| yield value }
        ensure
          close
        end
      end

      # Terminates the stream: no more elements are pulled from the input,
      # buffered results are released, and a consumer blocked waiting for
      # the next value is woken with a {LifecycleError}. Idempotent.
      #
      # @return [self]
      def close
        @Lock.synchronize do
          @Aborted = true
          @Results.clear
          @Lock.ns_broadcast
        end
        self
      end

      # @return [Boolean] true once the stream has been terminated, either
      #   by finishing, by an error, or explicitly with {#close}
      def closed?
        @Lock.synchronize { @Aborted }
      end

      private

      # Consumer-side loop, runs on the iterating thread. Pulling from the
      # input and waiting for results is interleaved so that at most
      # @Window elements are in flight at any moment.
      def drain
        submitted = 0
        delivered = 0
        exhausted = false

        until exhausted && delivered == submitted
          while !exhausted && submitted - delivered < @Window
            begin
              item = @Items.next
            rescue StopIteration
              exhausted = true
            else
              schedule submitted, item
              submitted += 1
            end
          end

          next if delivered == submitted

          result = take_result delivered
          delivered += 1
          if Failed === result
            raise result.error
          else
            yield result.value
          end
        end
      end

      def schedule(index, item)
        accepted =
          begin
            @Executor.post do
              store_result index, run_task(item)
            end
          rescue RejectedExecutionError => e
            store_result index, Failed.new(e)
            true
          end
        store_result index, Failed.new(RejectedExecutionError.new('executor rejected the task')) unless accepted
      end

      def run_task(item)
        Completed.new @Task.call(item)
      rescue StandardError => e
        Failed.new e
      end

      def store_result(index, result)
        @Lock.synchronize do
          next if @Aborted # stream terminated, discard late results
          @Results[index] = result
          @Lock.ns_signal
        end
      end

      def take_result(index)
        @Lock.synchronize do
          @Lock.ns_wait_until { @Aborted || @Results.key?(index) }
          if @Aborted && !@Results.key?(index)
            raise LifecycleError, 'the stream was closed'
          end
          @Results.delete index
        end
      end
    end
  end
end
