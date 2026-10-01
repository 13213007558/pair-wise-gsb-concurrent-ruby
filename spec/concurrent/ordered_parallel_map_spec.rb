require 'concurrent/ordered_parallel_map'
require 'concurrent/atomic/atomic_fixnum'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/executor/fixed_thread_pool'

RSpec.describe Concurrent::OrderedParallelMap do

  let(:executor) { Concurrent::FixedThreadPool.new(8, auto_terminate: false) }

  after(:each) do
    executor.shutdown
    executor.wait_for_termination(pool_termination_timeout)
  end

  # A one-shot enumerator which counts how many elements were pulled.
  # It does not support #to_a of finite size semantics nor #rewind usage
  # by the subject (a second iteration would yield nothing new is not
  # required here -- pulling is strictly forward).
  def one_shot_enumerator(size, pulled = nil)
    Enumerator.new do |yielder|
      size.times do |i|
        pulled&.increment
        yielder << i
      end
    end
  end

  context 'configuration' do
    it 'exposes window and executor' do
      mapper = described_class.new(window: 3, executor: executor)
      expect(mapper.window).to eq 3
      expect(mapper.executor).to eq executor
    end

    it 'resolves global executors from symbols and defaults to :io' do
      expect(described_class.new(executor: :fast).executor).to eq Concurrent.global_fast_executor
      expect(described_class.new(window: 2).executor).to eq Concurrent.global_io_executor
      expect(described_class.new(window: 2).window).to eq 2
    end

    it 'rejects an invalid window' do
      expect { described_class.new(window: 0) }.to raise_error(ArgumentError)
      expect { described_class.new(window: -1) }.to raise_error(ArgumentError)
      expect { described_class.new(window: 1.5) }.to raise_error(ArgumentError)
    end

    it 'requires a mapping block' do
      expect { described_class.new.map([1, 2, 3]) }.to raise_error(ArgumentError)
    end

    it 'requires an each-able input' do
      expect { described_class.new.map(Object.new) { |i| i } }.to raise_error(ArgumentError)
    end
  end

  context 'ordering' do
    it 'delivers results in input order, equal to a serial map' do
      input   = one_shot_enumerator(200)
      results = described_class.new(window: 8, executor: executor).map(input) do |i|
        sleep(rand * 0.01) # scramble completion order
        i * 2
      end.to_a

      expect(results).to eq (0...200).map { |i| i * 2 }
    end

    it 'behaves like a serial map with window 1' do
      results = described_class.new(window: 1, executor: executor).map(one_shot_enumerator(50)) { |i| i + 1 }.to_a
      expect(results).to eq (1..50).to_a
    end

    it 'handles nil and false values' do
      results = described_class.new(window: 4, executor: executor).map(one_shot_enumerator(3)) { |i| [nil, false, i][i] }.to_a
      expect(results).to eq [nil, false, 2]
    end

    it 'handles an empty input' do
      expect(described_class.new(window: 4, executor: executor).map([].each) { |i| i }.to_a).to eq []
    end
  end

  context 'laziness and input constraints' do
    it 'pulls nothing from the input until iterated' do
      pulled = Concurrent::AtomicFixnum.new(0)
      described_class.new(window: 4, executor: executor).map(one_shot_enumerator(10, pulled)) { |i| i }
      sleep 0.2
      expect(pulled.value).to eq 0
    end

    it 'never calls #to_a or #rewind on the input' do
      input = Object.new
      def input.each
        return to_enum(:each) unless block_given?
        5.times { |i| yield i }
      end

      def input.to_a
        raise 'to_a must not be called'
      end

      def input.rewind
        raise 'rewind must not be called'
      end

      results = described_class.new(window: 2, executor: executor).map(input) { |i| i * 3 }.to_a
      expect(results).to eq [0, 3, 6, 9, 12]
    end

    it 'can be consumed only once' do
      stream = described_class.new(window: 2, executor: executor).map(one_shot_enumerator(3)) { |i| i }
      expect(stream.to_a).to eq [0, 1, 2]
      expect { stream.each { |i| i } }.to raise_error(Concurrent::LifecycleError)
    end
  end

  context 'window bound' do
    it 'never exceeds the window while the first item is blocked and later items complete' do
      window    = 4
      release   = Concurrent::CountDownLatch.new
      pulled    = Concurrent::AtomicFixnum.new(0)
      completed = Concurrent::AtomicFixnum.new(0)
      delivered = Concurrent::AtomicFixnum.new(0)

      stream = described_class.new(window: window, executor: executor).map(one_shot_enumerator(10_000, pulled)) do |i|
        release.wait(10) if i == 0 # first item is stuck until released
        completed.increment
        i * 2
      end

      results  = []
      consumer = in_thread { stream.each { |v| delivered.increment; results << v } }

      begin
        # Let later tasks race ahead of the blocked first one.
        repeat_until_success(5) { expect(completed.value).to eq window - 1 }
        sleep 0.3

        # In-flight (running + completed-but-undelivered) stays within the window:
        # the input is not pulled further and nothing is delivered out of order.
        expect(pulled.value).to eq window
        expect(completed.value).to eq window - 1
        expect(delivered.value).to eq 0
        expect(pulled.value - delivered.value).to be <= window
      ensure
        release.count_down
      end

      expect(consumer.join(15)).not_to be_nil
      # After releasing the first item, order still matches a serial map.
      expect(results).to eq (0...10_000).map { |i| i * 2 }
    end
  end

  context 'termination' do
    it 're-raises input errors to the consumer and stops pulling' do
      pulled = Concurrent::AtomicFixnum.new(0)
      input  = Enumerator.new do |yielder|
        0.upto(Float::INFINITY) do |i|
          pulled.increment
          raise 'input boom' if i == 5
          yielder << i
        end
      end

      stream  = described_class.new(window: 2, executor: executor).map(input) { |i| i }
      results = []
      expect { stream.each { |v| results << v } }.to raise_error('input boom')

      expect(results).to eq [0, 1, 2, 3]
      expect(pulled.value).to eq 6 # pulled 0..5, the 6th pull raised
      expect(stream).to be_closed

      sleep 0.2
      expect(pulled.value).to eq 6 # nothing more is pulled after the error
    end

    it 're-raises task failures in order after earlier results' do
      stream  = described_class.new(window: 4, executor: executor).map(one_shot_enumerator(100)) do |i|
        raise 'task boom' if i == 3
        i * 10
      end

      results = []
      expect { stream.each { |v| results << v } }.to raise_error('task boom')
      expect(results).to eq [0, 10, 20]
      expect(stream).to be_closed
    end

    it 'stops pulling and releases buffered results when the consumer breaks early' do
      pulled = Concurrent::AtomicFixnum.new(0)
      stream = described_class.new(window: 3, executor: executor).map(one_shot_enumerator(1_000_000, pulled)) { |i| i }

      taken = []
      stream.each do |v|
        taken << v
        break if v == 3
      end

      expect(taken).to eq [0, 1, 2, 3]
      expect(stream).to be_closed
      pulled_at_break = pulled.value
      expect(pulled_at_break).to be <= 3 + 1 + 3 # delivered + in-flight <= window bound
      sleep 0.2
      expect(pulled.value).to eq pulled_at_break
    end

    it 'wakes a blocked consumer with LifecycleError when closed from another thread' do
      release = Concurrent::CountDownLatch.new
      stream  = described_class.new(window: 2, executor: executor).map(one_shot_enumerator(10)) do |i|
        release.wait(10) if i == 0
        i
      end

      error    = nil
      consumer = in_thread do
        begin
          stream.each { |v| v }
        rescue => e
          error = e
        end
      end

      repeat_until_success(5) { expect(consumer.status).to eq 'sleep' }
      stream.close
      expect(consumer.join(5)).not_to be_nil
      expect(error).to be_a(Concurrent::LifecycleError)
    ensure
      release.count_down
    end
  end
end
