require 'concurrent/scan'
require 'concurrent/executor/fixed_thread_pool'
require 'concurrent/atomic/count_down_latch'

RSpec.describe Concurrent::Scan do

  let!(:pool) { Concurrent::FixedThreadPool.new(4) }

  after(:each) do
    pool.shutdown
    pool.wait_for_termination(10)
  end

  def serial_inclusive(data, identity, &op)
    data.each_with_object([]) do |element, out|
      out << (out.empty? ? op.call(identity, element) : op.call(out.last, element))
    end
  end

  def serial_exclusive(data, identity, &op)
    accumulator = identity
    data.each_with_object([]) do |element, out|
      out << accumulator
      accumulator = op.call(accumulator, element)
    end
  end

  # 2x2 matrix multiplication: associative but not commutative.
  def matrix_multiply(left, right)
    [[left[0][0] * right[0][0] + left[0][1] * right[1][0],
      left[0][0] * right[0][1] + left[0][1] * right[1][1]],
     [left[1][0] * right[0][0] + left[1][1] * right[1][0],
      left[1][0] * right[0][1] + left[1][1] * right[1][1]]]
  end

  let(:matrix_identity) { [[1, 0], [0, 1]] }
  let(:matrices) do
    rng = Random.new(42)
    Array.new(37) { Array.new(2) { Array.new(2) { rng.rand(-5..5) } } }
  end

  describe '.inclusive_scan' do
    it 'returns every prefix in the original order' do
      data   = (1..100).to_a
      result = described_class.inclusive_scan(data, identity: 0, executor: pool) { |a, b| a + b }.value!
      expect(result).to eq serial_inclusive(data, 0) { |a, b| a + b }
      expect(result.size).to eq data.size
    end

    it 'matches a serial left fold for a non-commutative operation' do
      result = described_class.inclusive_scan(matrices, identity: matrix_identity, executor: pool, &method(:matrix_multiply)).value!
      expect(result).to eq serial_inclusive(matrices, matrix_identity, &method(:matrix_multiply))
    end

    it 'does not merge in task completion order' do
      # Make early chunks finish last; the result must still be in index order.
      data   = Array.new(64) { |i| "w#{i}" }
      result = described_class.inclusive_scan(data, identity: '', executor: pool, chunks: 8) do |acc, word|
        sleep 0.05 if word == 'w0' || acc.empty? # first chunk is the slowest
        acc + word
      end.value!
      expect(result).to eq serial_inclusive(data, '') { |a, b| a + b }
    end

    it 'fulfills immediately with an empty array for empty input' do
      future = described_class.inclusive_scan([], identity: 0, executor: pool) { |a, b| a + b }
      expect(future).to be_fulfilled
      expect(future.value).to eq []
    end

    it 'handles empty chunks when chunks exceed the input size' do
      data   = [1, 2]
      result = described_class.inclusive_scan(data, identity: 0, executor: pool, chunks: 8) { |a, b| a + b }.value!
      expect(result).to eq [1, 3]
    end

    it 'handles a single chunk' do
      data   = (1..10).to_a
      result = described_class.inclusive_scan(data, identity: 0, executor: pool, chunks: 1) { |a, b| a + b }.value!
      expect(result).to eq serial_inclusive(data, 0) { |a, b| a + b }
    end

    it 'handles more chunks than worker threads without stalling the executor' do
      single = Concurrent::FixedThreadPool.new(1)
      begin
        data   = (1..200).to_a
        future = described_class.inclusive_scan(data, identity: 0, executor: single, chunks: 50) { |a, b| a + b }
        future.wait(10)
        expect(future).to be_fulfilled
        expect(future.value).to eq serial_inclusive(data, 0) { |a, b| a + b }
      ensure
        single.shutdown
        single.wait_for_termination(10)
      end
    end

    it 'computes on the executor, not on the calling thread' do
      single  = Concurrent::FixedThreadPool.new(1)
      blocker = Concurrent::CountDownLatch.new(1)
      begin
        gate = Concurrent::Promises.future_on(single) { blocker.wait(10) }
        data = (1..50).to_a
        # The executor is occupied, so nothing can have been computed yet.
        future = described_class.inclusive_scan(data, identity: 0, executor: single) { |a, b| a + b }
        sleep 0.1
        expect(future).not_to be_resolved
        blocker.count_down
        gate.wait(10)
        future.wait(10)
        expect(future.value).to eq serial_inclusive(data, 0) { |a, b| a + b }
      ensure
        single.shutdown
        single.wait_for_termination(10)
      end
    end

    it 'rejects with the original exception when a chunk fails' do
      boom   = RuntimeError.new('chunk 3 blew up')
      future = described_class.inclusive_scan((1..100).to_a, identity: 0, executor: pool, chunks: 10) do |a, b|
        raise boom if b == 33
        a + b
      end
      expect { future.value! }.to raise_error(boom)
      expect(future.reason).to equal boom
    end

    it 'lets already scheduled work finish after a failure without hanging the executor' do
      single = Concurrent::FixedThreadPool.new(1)
      begin
        future = described_class.inclusive_scan((1..100).to_a, identity: 0, executor: single, chunks: 20) do |a, b|
          raise 'early failure' if b == 5
          a + b
        end
        future.wait(10)
        expect(future).to be_rejected
        # The executor is still usable afterwards.
        probe = Concurrent::Promises.future_on(single) { :alive }
        probe.wait(10)
        expect(probe.value).to eq :alive
      ensure
        single.shutdown
        single.wait_for_termination(10)
      end
    end

    it 'raises ArgumentError without a block or with invalid chunks' do
      expect { described_class.inclusive_scan([1], identity: 0, executor: pool) }.to raise_error(ArgumentError)
      expect do
        described_class.inclusive_scan([1], identity: 0, executor: pool, chunks: 0) { |a, b| a + b }
      end.to raise_error(ArgumentError)
    end
  end

  describe '.exclusive_scan' do
    it 'returns prefixes of the elements before each position' do
      data   = (1..100).to_a
      result = described_class.exclusive_scan(data, identity: 0, executor: pool) { |a, b| a + b }.value!
      expect(result).to eq serial_exclusive(data, 0) { |a, b| a + b }
      expect(result.first).to eq 0
      expect(result.size).to eq data.size
    end

    it 'matches a serial left fold for a non-commutative operation' do
      result = described_class.exclusive_scan(matrices, identity: matrix_identity, executor: pool, &method(:matrix_multiply)).value!
      expect(result).to eq serial_exclusive(matrices, matrix_identity, &method(:matrix_multiply))
      expect(result.first).to eq matrix_identity
    end

    it 'fulfills immediately with an empty array for empty input' do
      future = described_class.exclusive_scan([], identity: 0, executor: pool) { |a, b| a + b }
      expect(future).to be_fulfilled
      expect(future.value).to eq []
    end

    it 'handles empty chunks and a single chunk' do
      expect(described_class.exclusive_scan([1, 2], identity: 0, executor: pool, chunks: 8) { |a, b| a + b }.value!)
        .to eq [0, 1]
      expect(described_class.exclusive_scan([1, 2, 3], identity: 0, executor: pool, chunks: 1) { |a, b| a + b }.value!)
        .to eq [0, 1, 3]
    end

    it 'rejects with the original exception when a chunk fails' do
      future = described_class.exclusive_scan((1..50).to_a, identity: 0, executor: pool, chunks: 5) do |a, b|
        raise 'broken chunk' if b == 42
        a + b
      end
      expect { future.value! }.to raise_error(RuntimeError, 'broken chunk')
    end
  end
end
