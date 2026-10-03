require 'concurrent/promises'
require 'concurrent/errors'

module Concurrent

  # Parallel prefix operations.
  #
  # The supplied operation must be associative but does not have to be
  # commutative. Combination tasks are therefore linked by their logical
  # positions; a task is posted only after the exact futures on which it
  # depends have fulfilled.
  #
  # @example Non-commutative string concatenation
  #   merges = []
  #   operation = ->(left, right) { merges << [left, right]; left + right }
  #   future = Concurrent.scan_inclusive(%w[a b c d], '', executor, 4, &operation)
  #   future.value! #=> ['a', 'ab', 'abc', 'abcd']
  #
  #   # Every recorded pair is a positional task dependency. Completion order is
  #   # irrelevant; the operands still appear as left then right. The singleton
  #   # case records 7 combinations: 4 in the block tree and 3 when applying
  #   # block prefixes. The longest dependency chain has 3 combinations.
  module Scan
    extend self

    # Computes all inclusive prefixes.
    #
    # @param [Array<Object>] elements input values in logical order
    # @param [Object] identity identity element of `operation`
    # @param [#post] executor bounded executor used for scan tasks
    # @param [Integer, nil] chunk_count number of chunks; defaults to the
    #   executor's `max_length`
    # @yield [left, right] combines ordered partial results
    # @return [Concurrent::Promises::Future<Array<Object>>] all prefixes in
    #   original order
    def inclusive(elements, identity, executor, chunk_count = nil, &operation)
      scan(elements, identity, executor, chunk_count, false, operation)
    end

    # Computes all exclusive prefixes. The result at position `i` is the
    # combination of positions before `i`; the first result is `identity`.
    #
    # @param [Array<Object>] elements input values in logical order
    # @param [Object] identity identity element of `operation`
    # @param [#post] executor bounded executor used for scan tasks
    # @param [Integer, nil] chunk_count number of chunks; defaults to the
    #   executor's `max_length`
    # @yield [left, right] combines ordered partial results
    # @return [Concurrent::Promises::Future<Array<Object>>] all prefixes in
    #   original order
    def exclusive(elements, identity, executor, chunk_count = nil, &operation)
      scan(elements, identity, executor, chunk_count, true, operation)
    end

    private

    def scan(elements, identity, executor, requested_chunks, exclusive, operation)
      chunk_count = validate_arguments(elements, executor, requested_chunks, operation)
      return Promises.fulfilled_future([], :immediate) if elements.empty?

      chunks = partition(elements, chunk_count)
      first_nonempty_chunk = chunks.index { |chunk| !chunk.empty? }
      identity_future = Promises.fulfilled_future(identity, :immediate)

      local_futures = chunks.map do |chunk|
        if chunk.empty?
          Promises.fulfilled_future([], :immediate)
        else
          schedule(executor, []) { inclusive_local_scan(chunk, operation) }
        end
      end

      total_futures = chunks.each_with_index.map do |chunk, index|
        next identity_future if chunk.empty?

        schedule(executor, [local_futures[index]]) do |local|
          local.last
        end
      end

      block_prefixes = build_block_prefixes(
        chunks, total_futures, identity_future, executor, operation
      )

      result_futures = chunks.each_with_index.map do |chunk, index|
        next Promises.fulfilled_future([], :immediate) if chunk.empty?

        schedule(executor, [local_futures[index], block_prefixes[index]]) do |local, prefix|
          if !exclusive && index == first_nonempty_chunk
            local
          elsif exclusive && index == first_nonempty_chunk
            [identity, *local[0...-1]]
          elsif exclusive
            [prefix, *local[0...-1].map { |value| operation.call(prefix, value) }]
          else
            local.map { |value| operation.call(prefix, value) }
          end
        end
      end

      schedule(executor, result_futures) { |*results| results.reduce([], :concat) }
    end

    def validate_arguments(elements, executor, requested_chunk_count, operation)
      raise ArgumentError, 'elements must be an Array' unless elements.is_a?(::Array)
      raise ArgumentError, 'executor must respond to #post' unless executor.respond_to?(:post)
      raise ArgumentError, 'no block given' unless operation

      chunk_count = requested_chunk_count
      if chunk_count.nil?
        unless executor.respond_to?(:max_length)
          raise ArgumentError, 'chunk_count is required when the executor has no max_length'
        end
        chunk_count = executor.max_length
      end

      unless chunk_count.is_a?(Integer) && chunk_count > 0
        raise ArgumentError, 'chunk_count must be a positive Integer'
      end

      chunk_count
    end

    def partition(elements, chunk_count)
      size = elements.size
      base_size = size / chunk_count
      larger_count = size % chunk_count
      offset = 0

      chunk_count.times.map do |index|
        current_size = base_size + (index < larger_count ? 1 : 0)
        chunk = elements[offset, current_size]
        offset += current_size
        chunk
      end
    end

    def inclusive_local_scan(chunk, operation)
      chunk.each_with_object([]) do |element, values|
        values << (values.empty? ? element : operation.call(values.last, element))
      end
    end

    def build_block_prefixes(chunks, total_futures, identity_future, executor, operation)
      prefixes = ::Array.new(chunks.size) { identity_future }
      nonempty_totals = []
      compact_index_by_chunk = ::Array.new(chunks.size)
      compact_index = 0
      chunks.each_with_index do |chunk, index|
        if chunk.empty?
          prefixes[index] = identity_future
          next
        end

        compact_index_by_chunk[index] = compact_index
        nonempty_totals << total_futures[index]
        compact_index += 1
      end

      compact_prefixes, = compact_exclusive_prefixes(
        nonempty_totals, identity_future, executor, operation
      )
      seen_nonempty = 0
      chunks.each_with_index do |chunk, index|
        if chunk.empty?
          prefixes[index] = compact_prefixes[seen_nonempty] if seen_nonempty < nonempty_totals.size
        else
          prefixes[index] = compact_prefixes[compact_index_by_chunk[index]]
          seen_nonempty += 1
        end
      end

      prefixes
    end

    def compact_exclusive_prefixes(total_futures, identity_future, executor, operation)
      return [[identity_future], total_futures.first] if total_futures.size == 1

      split = total_futures.size / 2
      left_prefixes, left_total = compact_exclusive_prefixes(
        total_futures[0...split], identity_future, executor, operation
      )
      right_prefixes, right_total = compact_exclusive_prefixes(
        total_futures[split..-1], identity_future, executor, operation
      )
      total = schedule(executor, [left_total, right_total]) do |left, right|
        operation.call(left, right)
      end

      right_prefixes = right_prefixes.each_with_index.map do |prefix, index|
        next left_total if index.zero?
        schedule(executor, [left_total, prefix]) do |left, right|
          operation.call(left, right)
        end
      end

      [left_prefixes + right_prefixes, total]
    end

    def schedule(executor, dependencies, &task)
      result = Promises.resolvable_future_on(:immediate)
      gate = if dependencies.empty?
               Promises.resolved_event(:immediate)
             else
               Promises.zip_events_on(:immediate, *dependencies)
             end

      gate.chain_on(:immediate) do
        rejected = dependencies.find { |dependency| !dependency.fulfilled? }
        if rejected
          result.reject(rejected.reason)
          next
        end

        begin
          posted = executor.post do
            begin
              value = task.call(*dependencies.map(&:value))
              result.fulfill(value)
            rescue Exception => error
              result.reject(error)
            end
          end
          result.reject(RejectedExecutionError.new('executor rejected task')) unless posted
        rescue => error
          result.reject(error)
        end
      end

      result.with_hidden_resolvable
    end
  end

  class << self
    # Asynchronously computes all prefixes of `elements`.
    #
    # With balanced chunk sizes and `p` non-empty chunks, local scans perform
    # `n - p` combinations. The balanced block-prefix tree performs fewer than
    # `2 * p` combinations, and applying block prefixes performs `n - p`.
    # Total work is O(n); the critical path is O(n / p + log p).
    def scan(elements, identity, executor, chunk_count = nil, exclusive: false, &operation)
      if exclusive
        Scan.exclusive(elements, identity, executor, chunk_count, &operation)
      else
        Scan.inclusive(elements, identity, executor, chunk_count, &operation)
      end
    end

    def scan_inclusive(elements, identity, executor, chunk_count = nil, &operation)
      Scan.inclusive(elements, identity, executor, chunk_count, &operation)
    end

    def scan_exclusive(elements, identity, executor, chunk_count = nil, &operation)
      Scan.exclusive(elements, identity, executor, chunk_count, &operation)
    end
  end
end
