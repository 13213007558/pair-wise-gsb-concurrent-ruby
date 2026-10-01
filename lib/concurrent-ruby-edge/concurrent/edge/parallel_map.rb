require 'concurrent'
require 'concurrent/promises'

module Concurrent
  module Edge
    # Raised by {Concurrent::Edge::ParallelMap} when pulling the next element
    # from the source Enumerable raises. The original exception is available as
    # `#cause`.
    class SourceEnumerationError < StandardError
      def initialize(message, cause: nil)
        super(message)
        @cause = cause
      end

      attr_reader :cause
    end

    # Lazily and concurrently transforms a long, one-shot Enumerable (typically
    # an Enumerator) with a bounded "window" of in-flight work.
    #
    # Unlike a parallel map which eagerly schedules every element (e.g. by
    # calling `to_a` on the source), the source is pulled lazily: at most
    # `window` elements are ever held at the same time as futures which are
    # executing or have finished but whose results have not been delivered to
    # the consumer yet. The memory footprint therefore depends on `window`,
    # never on the length of the source.
    #
    # Results are delivered in the source order even though the tasks run and
    # complete out of order on the executor. The source is only required to
    # respond to `#each` (or to be an Enumerator); it is never rewound and is
    # consumed at most once.
    #
    # Termination behaviour:
    #
    # * The source raising while producing an element stops scheduling new
    #   tasks; elements already pulled are delivered in order, then a
    #   {SourceEnumerationError} (wrapping the original error) is raised.
    # * A task raising stops scheduling new tasks; already pulled results are
    #   delivered in order, then the task's exception is raised.
    # * The consumer stopping early (e.g. `break`, `raise` or discarding the
    #   enumeration) immediately stops pulling the source. Tasks already
    #   scheduled are awaited so no task is abandoned mid-flight, their results
    #   are discarded, and the exception (if any) is propagated. The executor
    #   itself is never shut down.
    #
    # @!macro warn.edge
    #
    # @example
    #   rows = Concurrent::Edge::ParallelMap.new(
    #     huge_enumerator, window: 8,
    #     executor: Concurrent.global_io_executor
    #   ) { |row| fetch(row) }
    #
    #   rows.each { |result| store(result) }
    class ParallelMap
      include Enumerable

      # @param [Enumerable] source a lazy, possibly one-shot sequence of input
      #   elements. It is consumed once via `#each` / `#next`, never rewound,
      #   and never converted to an array.
      # @param [Integer] window maximum number of elements which may be
      #   scheduled and not yet delivered at any point. Must be positive.
      # @param [ExecutorService, :io, :fast, :immediate, nil] executor the
      #   executor the mapping tasks run on. Defaults to the global io executor.
      # @yield [element] block transforming each element.
      def initialize(source, window: 4, executor: nil, &block)
        raise ArgumentError, 'no block given' unless block
        raise ArgumentError, 'window must be positive' unless window.is_a?(Integer) && window.positive?

        @source   = source
        @window   = window
        @executor = executor || Concurrent.global_io_executor
        @block    = block
        @mutex    = Mutex.new
        @done     = false
      end

      # @return [Integer] the maximum number of in-flight (running or finished
      #   but undelivered) elements.
      attr_reader :window

      # @return [ExecutorService] the executor tasks are scheduled on.
      attr_reader :executor

      # Yields the mapped results in the source order. Pulls elements from the
      # source lazily, never keeping more than {#window} of them in flight.
      #
      # @raise [SourceEnumerationError] if the source raises while enumerating.
      # @raise [Exception] the exception raised by a mapping task.
      # @return [Enumerator] a lazy enumerator when called without a block.
      def each(&block)
        return to_enum(:each) unless block

        @mutex.synchronize do
          raise ArgumentError, 'the one-shot source has already been consumed' if @done
          @done = true
        end

        enumerator =
          if @source.is_a?(Enumerator)
            @source
          else
            @source.to_enum(:each)
          end

        pending        = []
        source_closed  = false
        source_failure = nil

        refill = lambda do
          while !source_closed && pending.length < @window
            begin
              element = enumerator.next
            rescue StopIteration
              source_closed = true
              break
            rescue Exception => e
              # Preserve the source error for after the scheduled results are
              # delivered; stop pulling the source immediately.
              source_closed  = true
              source_failure = e
              break
            end
            pending << schedule(element)
          end
        end

        refill.call

        begin
          loop do
            if pending.empty?
              if source_failure
                raise SourceEnumerationError.new(
                        "the source enumerator raised: #{source_failure.class}: #{source_failure.message}",
                        cause: source_failure
                      )
              end
              break
            end

            future = pending.shift
            future.wait
            if future.fulfilled?
              yield future.value
            else
              # Deliver nothing for the failed task; drain the rest so no task
              # is abandoned, then surface the task's exception.
              pending.each(&:wait)
              raise future.reason
            end

            # A slot freed up by delivering the result; pull more lazily.
            refill.call
          end
        ensure
          # Covers the consumer stopping early (break/raise/thread kill): the
          # source is not pulled anymore, wait for everything already scheduled
          # and discard its result.
          pending.each(&:wait)
        end

        self
      end

      private

      def schedule(element)
        Concurrent::Promises.future_on(@executor, element, &@block)
      end
    end
  end
end
