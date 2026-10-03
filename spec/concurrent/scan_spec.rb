require 'concurrent/scan'
require 'concurrent/executor/fixed_thread_pool'

module Concurrent
  RSpec.describe '.scan' do
    let(:executor) { FixedThreadPool.new(2) }

    after do
      executor.shutdown
      expect(executor.wait_for_termination(5)).to eq(true)
    end

    it 'returns an empty inclusive result without scheduling work' do
      future = Concurrent.scan_inclusive([], 0, executor, 1) { raise 'must not run' }

      expect(future).to be_a(Promises::Future)
      expect(future.value!(1)).to eq([])
    end

    it 'returns an empty exclusive result without scheduling work' do
      future = Concurrent.scan_exclusive([], 0, executor, 1) { raise 'must not run' }

      expect(future).to be_a(Promises::Future)
      expect(future.value!(1)).to eq([])
    end

    it 'supports one chunk for inclusive scan' do
      future = Concurrent.scan_inclusive([1, 2, 3], 0, executor, 1, &:+)

      expect(future.value!(1)).to eq([1, 3, 6])
    end

    it 'supports one chunk for exclusive scan' do
      future = Concurrent.scan_exclusive([1, 2, 3], 0, executor, 1, &:+)

      expect(future.value!(1)).to eq([0, 1, 3])
    end

    it 'accepts the inclusive and exclusive keyword form' do
      inclusive = Concurrent.scan([1, 2], 0, executor, 2, exclusive: false, &:+).value!(1)
      exclusive = Concurrent.scan([1, 2], 0, executor, 2, exclusive: true, &:+).value!(1)

      expect(inclusive).to eq([1, 3])
      expect(exclusive).to eq([0, 1])
    end

    it 'preserves original order for non-commutative string concatenation' do
      inclusive = Concurrent.scan_inclusive(%w[a b c d e], '', executor, 2, &:+).value!(1)
      exclusive = Concurrent.scan_exclusive(%w[a b c d e], '', executor, 2, &:+).value!(1)

      expect(inclusive).to eq(%w[a ab abc abcd abcde])
      expect(exclusive).to eq(['', 'a', 'ab', 'abc', 'abcd'])
    end

    it 'defines empty chunks when there are more chunks than elements' do
      inclusive = Concurrent.scan_inclusive(%w[a b c], '', executor, 7, &:+).value!(1)
      exclusive = Concurrent.scan_exclusive(%w[a b c], '', executor, 7, &:+).value!(1)

      expect(inclusive).to eq(%w[a ab abc])
      expect(exclusive).to eq(['', 'a', 'ab'])
    end

    it 'does not combine partial results in task completion order' do
      merge_count = 0
      merges = []
      operation = lambda do |left, right|
        merge_count += 1
        merges << [left, right]
        left + right
      end

      values = %w[aa bb cc dd ee]
      future = Concurrent.scan_inclusive(values, '', executor, 5, &operation)

      expect(future.value!(1)).to eq(%w[aa aabb aabbcc aabbccdd aabbccddee])
      expect(merges).to include(['aa', 'bb'], ['aabb', 'cc'])
      expect(merges).not_to include(['bb', 'aa'])
      expect(merge_count).to eq(11)
    end

    it 'records the block tree merge count and dependencies' do
      merges = []
      operation = lambda do |left, right|
        merges << [left, right]
        left + right
      end

      future = Concurrent.scan_inclusive(%w[aa bb cc dd ee], '', executor, 5, &operation)
      future.value!(1)
      expected_prefix_merges = [
        ['aa', 'bb'],
        ['cc', 'dd'],
        ['dd', 'ee'],
        ['aabb', 'cc'],
        ['cc', 'ddee'],
        ['aabb', 'ccdd'],
        ['aabb', 'ccddee']
      ]
      expected_apply_merges = [
        ['aabb', 'cc'],
        ['aabbcc', 'dd'],
        ['aabbccdd', 'ee'],
        ['aa', 'bb']
      ]

      expect(merges).to match_array(expected_prefix_merges + expected_apply_merges)
      expect(merges.size).to eq(11)
    end

    it 'documents the singleton four-chunk merge edges' do
      merges = []
      operation = lambda do |left, right|
        merges << [left, right]
        left + right
      end

      Concurrent.scan_inclusive(%w[a b c d], '', executor, 4, &operation).value!(1)

      expect(merges).to match_array([
        ['a', 'b'],
        ['c', 'd'],
        ['ab', 'c'],
        ['a', 'b'],
        ['ab', 'c'],
        ['ab', 'cd'],
        ['abc', 'd']
      ])
    end

    it 'rejects the whole result when a chunk fails' do
      error = Class.new(StandardError)
      future = Concurrent.scan_inclusive([1, 2, 3], 0, executor, 3) do |_, _|
        raise error, 'boom'
      end

      expect { future.value!(1) }.to raise_error(error, 'boom')
    end

    it 'propagates non-StandardError failures without leaving work pending' do
      error = Class.new(Exception)
      future = Concurrent.scan_inclusive([1, 2], 0, executor, 2) { raise error, 'fatal' }

      expect { future.value!(1) }.to raise_error(error, 'fatal')
      expect(executor.completed_task_count).to be > 0
    end

    it 'fails when the bounded executor rejects a task' do
      rejecting_executor = Object.new
      def rejecting_executor.post(*_args)
        false
      end
      future = Concurrent.scan_inclusive([1], 0, rejecting_executor, 1, &:+)

      expect { future.value!(1) }.to raise_error(RejectedExecutionError)
    end

    it 'defaults chunk_count to the bounded executor max_length' do
      pool = FixedThreadPool.new(1)
      future = Concurrent.scan_inclusive([1, 2, 3], 0, pool, &:+)

      expect(future.value!(1)).to eq([1, 3, 6])
      pool.shutdown
      expect(pool.wait_for_termination(5)).to eq(true)
    end

    it 'requires a positive chunk count' do
      expect do
        Concurrent.scan_inclusive([1], 0, executor, 0, &:+)
      end.to raise_error(ArgumentError, 'chunk_count must be a positive Integer')
    end

    it 'requires an operation' do
      expect do
        Concurrent.scan_inclusive([1], 0, executor, 1)
      end.to raise_error(ArgumentError, 'no block given')
    end
  end
end
