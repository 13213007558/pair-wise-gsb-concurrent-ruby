require 'concurrent/promises'
require 'concurrent/utility/processor_counter'

module Concurrent

  # Asynchronous parallel prefix scan (all-prefix-sums) over any ordered
  # collection, generalized from the "reduce" idea: instead of a single
  # total, a scan returns the accumulated result *before every position*.
  #
  # The binary operation must be **associative** and the given identity must
  # be a true neutral element for it (+op.call(identity, x) == x+ and
  # +op.call(x, identity) == x+). The operation does **not** need to be
  # commutative: partial results are always combined in index order, never in
  # task-completion order, so non-commutative operations such as matrix
  # multiplication or string concatenation produce the same prefixes as a
  # left-to-right serial scan.
  #
  # All computation is scheduled on the supplied (typically bounded) executor
  # as a graph of {Promises} futures. The calling thread only builds that
  # graph; it never computes the prefixes itself and never wraps an
  # already-computed serial result into a future. Internally no task ever
  # calls +wait+/+value+ on another task — dependencies are expressed purely
  # through promise chaining — so internal waits can never occupy or deadlock
  # a bounded executor, even with a single worker thread and many chunks.
  #
  # ## Algorithm
  #
  # A two-level (chunked) scan with three phases, each phase a set of
  # non-blocking tasks on the executor:
  #
  # 1. **Totals** — one task per chunk folds the chunk left-to-right into a
  #    single partial total (empty chunks yield the identity).
  # 2. **Chunk prefixes** — one task combines the chunk totals *in index
  #    order* into the exclusive prefix before each chunk.
  # 3. **Rescan** — one task per chunk emits the final prefixes within the
  #    chunk, seeded with the chunk's exclusive prefix.
  #
  # With +n+ elements split into +c+ chunks this applies the operation
  # +2n - c_nonempty + c - 1+ times (O(n) total work, roughly twice a serial
  # scan) and the critical path is about +2 * ceil(n/c) + c+ sequential
  # applications (O(n/c + c)), versus O(n) for a serial scan.
  #
  # ## Edge cases
  #
  # * Empty input scans to an empty array (fulfilled immediately).
  # * Empty chunks (when +chunks+ exceeds the input size) are well defined:
  #   their total is the identity and they contribute no output elements.
  # * A single chunk degenerates to a serial scan, still on the executor.
  # * More chunks than executor threads is fine: tasks simply queue, since
  #   no task ever blocks waiting for another.
  # * If any chunk task fails, the returned future is rejected with the
  #   original exception of the first failing chunk (in index order); the
  #   already-scheduled dependent tasks resolve without running their blocks
  #   and without occupying executor threads.
  #
  # @see Promises
  module Scan
    extend self

    # Computes the inclusive prefix scan: element +i+ of the result is
    # +op(op(...op(identity, input[0])..., input[i-1]), input[i])+, i.e. the
    # accumulation of all elements up to and including position +i+.
    #
    # @param [#to_a] collection the ordered input elements
    # @param [Object] identity neutral element of the operation; it must not
    #   be mutated by the operation (it is shared between chunk tasks)
    # @param [Executor, :io, :fast] executor the executor all internal tasks
    #   are scheduled on; may be bounded to any size
    # @param [Integer, nil] chunks number of chunks to split the input into;
    #   defaults to twice the processor count, clamped to +[1, input size]+
    # @yield [accumulator, element] the associative binary operation, applied
    #   strictly in index order; must not mutate its arguments
    # @return [Promises::Future] fulfilled with an Array of the same size as
    #   the input, holding every prefix in the original order; rejected with
    #   the first failing chunk's exception if any chunk fails
    def inclusive_scan(collection, identity:, executor:, chunks: nil, &op)
      scan(collection, true, identity, executor, chunks, op)
    end

    # Computes the exclusive prefix scan: element +i+ of the result is the
    # accumulation of all elements *before* position +i+, so the first
    # element is always the identity. The result has the same size as the
    # input (and is empty for empty input).
    #
    # @param (see Scan.inclusive_scan)
    # @yield (see Scan.inclusive_scan)
    # @return (see Scan.inclusive_scan)
    def exclusive_scan(collection, identity:, executor:, chunks: nil, &op)
      scan(collection, false, identity, executor, chunks, op)
    end

    private

    def scan(collection, inclusive, identity, executor, chunks, op)
      raise ArgumentError, 'an associative binary operation block is required' unless op

      data = collection.to_a
      return Promises.resolved_future(true, [].freeze, nil, executor) if data.empty?

      slices = partition(data.size, normalize_chunks(chunks, data.size))

      # Phase 1: per-chunk totals, each chunk folded left-to-right.
      totals = slices.map do |range|
        Promises.future_on(executor, data, range, op, identity) do |elements, chunk, operation, zero|
          if chunk.begin >= chunk.end
            zero
          else
            accumulator = elements[chunk.begin]
            (chunk.begin + 1).upto(chunk.end - 1) do |index|
              accumulator = operation.call(accumulator, elements[index])
            end
            accumulator
          end
        end
      end

      # Phase 2: exclusive prefixes of the chunk totals. zip_futures_on
      # collects the totals in index order (not completion order), and the
      # combination itself runs as a task on the executor.
      chunk_prefixes = Promises.zip_futures_on(executor, *totals)
                               .then_on(executor, op, identity) do |*chunk_totals, operation, zero|
        prefixes    = []
        accumulator = zero
        last_index  = chunk_totals.size - 1
        chunk_totals.each_with_index do |total, index|
          prefixes    << accumulator
          accumulator = operation.call(accumulator, total) unless index == last_index
        end
        prefixes
      end

      # Phase 3: rescan each chunk, seeded with its exclusive prefix.
      scanned = slices.each_with_index.map do |range, index|
        chunk_prefixes.then_on(executor, data, range, index, op, inclusive) do |prefixes, elements, chunk, chunk_index, operation, emit_inclusive|
          output      = []
          accumulator = prefixes[chunk_index]
          chunk.each do |element_index|
            if emit_inclusive
              accumulator = operation.call(accumulator, elements[element_index])
              output      << accumulator
            else
              output      << accumulator
              accumulator = operation.call(accumulator, elements[element_index])
            end
          end
          output
        end
      end

      # Join the chunk outputs in index order and unwrap the failure reason
      # so a chunk failure rejects the whole scan with the original exception.
      Promises.zip_futures_on(executor, *scanned)
              .then_on(executor) { |*chunk_outputs| chunk_outputs.flatten(1) }
              .rescue_on(executor) { |reason| raise first_failure(reason) }
    end

    def normalize_chunks(chunks, size)
      count = chunks || 2 * Concurrent.processor_count
      count = count.to_i
      raise ArgumentError, "chunks must be a positive Integer, got #{chunks.inspect}" if count < 1
      [count, size].min
    end

    # Splits +0...size+ into +chunk_count+ contiguous ranges, as evenly as
    # possible; trailing ranges may be empty when chunk_count > size.
    def partition(size, chunk_count)
      base, extra = size.divmod(chunk_count)
      slices      = []
      start       = 0
      chunk_count.times do |index|
        length = base + (index < extra ? 1 : 0)
        slices << (start...(start + length))
        start  += length
      end
      slices
    end

    # Zip rejections nest reasons into arrays; dig out the first real one.
    def first_failure(reason)
      reason = reason.find { |nested| !nested.nil? } while reason.is_a?(::Array)
      reason.is_a?(Exception) ? reason : StandardError.new(reason.inspect)
    end
  end
end
