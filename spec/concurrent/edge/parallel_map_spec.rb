require 'concurrent/edge/parallel_map'

RSpec.describe 'Concurrent' do
  describe Concurrent::Edge::ParallelMap, edge: true do
    # Latch incremented each time the mapping block is entered, so tests can
    # observe how many elements have actually been scheduled.
    def counting_source(count, &on_next)
      Enumerator.new do |y|
        count.times do |i|
          on_next.call(i) if on_next
          y << i
        end
      end
    end

    def fresh_executor(size = 8)
      Concurrent::FixedThreadPool.new(size)
    end

    # Thread::Queue#to_a is not available on modern Ruby.
    def drain(queue)
      values = []
      while (value = queue.pop(true) rescue nil)
        values << value
      end
      values
    end

    after(:each) do
      @executor&.shutdown
      @executor&.wait_for_termination
    end

    specify 'exposes window and executor' do
      @executor     = fresh_executor(2)
      parallel_map  = described_class.new([1], window: 3, executor: @executor) { |i| i }

      expect(parallel_map.window).to eq 3
      expect(parallel_map.executor).to eq @executor
    end

    specify 'validates the window' do
      expect { described_class.new([1], window: 0) { |i| i } }
        .to raise_error(ArgumentError)
      expect { described_class.new([1], window: -1) { |i| i } }
        .to raise_error(ArgumentError)
    end

    specify 'requires a mapping block' do
      expect { described_class.new([1]) }.to raise_error(ArgumentError)
    end

    specify 'delivers results in source order with parallel completion' do
      @executor = fresh_executor(4)
      order     = []
      order_lock = Mutex.new
      source    = (0...50).each
      mapper    = described_class.new(source, window: 8, executor: @executor) do |i|
        sleep rand * 0.01
        order_lock.synchronize { order << i }
        i * 2
      end

      expect(mapper.to_a).to eq (0...50).map { |i| i * 2 }
    end

    specify 'the window does not grow while the first item is stuck, ' \
            'then drains in order once it is released' do
      @executor       = fresh_executor(6)
      window          = 3
      total           = 30
      scheduled       = Queue.new
      first_started   = Concurrent::Event.new
      release_first   = Concurrent::Event.new
      rest_completed  = Queue.new

      source = counting_source(total)

      mapper = described_class.new(source, window: window, executor: @executor) do |i|
        scheduled << i
        first_started.set if i.zero?
        if i.zero?
          release_first.wait(10) or raise 'first task was never released'
        else
          rest_completed << i
        end
        i
      end

      delivered = Queue.new
      consumer  = Thread.new do
        mapper.each { |v| delivered << v }
        delivered.close
      end

      # The window fills up: the stuck first task and the rest of the window.
      expect(first_started.wait(5)).to be_truthy

      # The following tasks finish very fast, but while the head of the queue
      # is stuck neither the scheduled set nor the completed-undelivered set
      # may keep growing: poll over time and assert the bound holds steadily.
      sleep 0.2
      5.times do
        expect(scheduled.length).to eq window
        expect(rest_completed.length).to eq(window - 1)
        expect(delivered.length).to eq 0
        sleep 0.05
      end

      # Release the stuck item; everything drains in exactly the order a serial
      # map would produce.
      release_first.set
      consumer.join(10) or raise 'consumer did not finish'

      serial_map = (0...total).map { |i| i }
      expect(drain(delivered)).to eq serial_map
    end

    specify 'pulls the one-shot source lazily and never rewinds it' do
      @executor = fresh_executor(2)
      pulled    = Queue.new

      source = Enumerator.new do |y|
        1000.times do |i|
          pulled << i
          y << i
        end
      end

      seen = 0
      window = 4
      described_class.new(source, window: window, executor: @executor) { |i| i }.each do |_|
        seen += 1
        # At most (delivered) + window source elements have been pulled ahead.
        expect(pulled.length).to be <= seen + window
        break if seen == 50
      end

      # Early stop: the source is not drained, proving it was pulled lazily.
      expect(seen).to eq 50
      expect(pulled.length).to be < 1000
    end

    specify 'does not require the source to respond to rewind' do
      @executor = fresh_executor(2)
      source    = Object.new
      def source.each
        3.times { |i| yield i }
      end

      mapper = described_class.new(source, window: 2, executor: @executor, &:succ)
      expect(mapper.to_a).to eq [1, 2, 3]
    end

    specify 'raises the task failure after delivering earlier results ' \
            'and stops pulling the source' do
      @executor = fresh_executor(4)
      pulled    = []
      pull_lock = Mutex.new
      source    = Enumerator.new do |y|
        20.times do |i|
          pull_lock.synchronize { pulled << i }
          y << i
        end
      end

      mapper = described_class.new(source, window: 5, executor: @executor) do |i|
        raise "boom at #{i}" if i == 3
        i
      end

      delivered = []
      expect do
        mapper.each { |v| delivered << v }
      end.to raise_error(RuntimeError, 'boom at 3')

      expect(delivered).to eq [0, 1, 2]
      # Scheduling stops right after the failure is observed; far from all 20
      # elements have been pulled.
      expect(pulled.max).to be < 20
    end

    specify 'wraps an error raised by the source itself' do
      @executor = fresh_executor(2)
      source    = Enumerator.new do |y|
        y << 1
        y << 2
        raise 'source exploded'
      end

      mapper    = described_class.new(source, window: 2, executor: @executor, &:succ)
      delivered = []
      expect do
        mapper.each { |v| delivered << v }
      end.to raise_error(Concurrent::Edge::SourceEnumerationError) do |err|
        expect(err.cause).to be_a(RuntimeError)
        expect(err.cause.message).to eq 'source exploded'
      end
      expect(delivered).to eq [2, 3]
    end

    specify 'stops pulling and discards undelivered results when the consumer breaks' do
      @executor     = fresh_executor(2)
      started       = Queue.new
      source        = counting_source(100) { |i| started << i }
      mapper        = described_class.new(source, window: 3, executor: @executor) do |i|
        sleep 0.05 unless i.zero?
        i
      end

      delivered = []
      mapper.each do |v|
        delivered << v
        break if v == 0
      end

      scheduled_count = started.length
      expect(delivered).to eq [0]
      # Nothing new is scheduled after the break; let the executor go idle and
      # confirm no further tasks appeared.
      sleep 0.3
      expect(started.length).to eq scheduled_count
      expect(scheduled_count).to be < 100
    end

    specify 'waits for already scheduled tasks on early stop and keeps the executor alive' do
      @executor = fresh_executor(2)
      ran       = Queue.new
      source    = counting_source(6)
      mapper    = described_class.new(source, window: 4, executor: @executor) do |i|
        sleep 0.1 if i > 1
        ran << i
        i
      end

      mapper.each do |v|
        raise 'consumer changed its mind' if v.zero?
      end rescue nil

      # In-flight tasks are awaited (not abandoned) and their results dropped.
      sleep 0.05
      expect(ran.length).to be >= 4
      expect(@executor).not_to be_shutdown
    end

    specify 'the source can only be consumed once' do
      @executor = fresh_executor(2)
      mapper    = described_class.new((1..3).each, window: 2, executor: @executor, &:succ)

      expect(mapper.to_a).to eq [2, 3, 4]
      expect { mapper.to_a }.to raise_error(ArgumentError, /already been consumed/)
    end

    specify 'works without a block as a lazy enumerator' do
      @executor = fresh_executor(2)
      mapper    = described_class.new((1..5).each, window: 2, executor: @executor, &:succ)

      expect(mapper.each).to be_a(Enumerator)
      expect(mapper.each.first(3)).to eq [2, 3, 4]
    end

    specify 'window 1 processes strictly serially' do
      @executor   = fresh_executor(4)
      in_flight   = Concurrent::AtomicFixnum.new(0)
      max_flight  = Concurrent::AtomicFixnum.new(0)
      mapper      = described_class.new((1...100).each, window: 1, executor: @executor) do |i|
        current = in_flight.increment
        max_flight.update { |max| [max, current].max }
        sleep rand * 0.002
        in_flight.decrement
        i * 3
      end

      expect(mapper.to_a).to eq (1...100).map { |i| i * 3 }
      expect(max_flight.value).to eq 1
    end

    specify 'keeps a long one-shot stream bounded regardless of its length' do
      @executor = fresh_executor(4)
      window    = 5
      length    = 10_000
      pulled    = Concurrent::AtomicFixnum.new(0)
      delivered_count = Concurrent::AtomicFixnum.new(0)
      source    = Enumerator.new do |y|
        length.times do |i|
          pulled.increment
          y << i
        end
      end

      mapper = described_class.new(source, window: window, executor: @executor) do |i|
        sleep 0.001 if (i % 97).zero?
        i
      end

      mapper.each do |i|
        # While consuming, the pull-ahead distance can never exceed the
        # delivered count plus the window.
        expect(pulled.value).to be <= delivered_count.value + window
        delivered_count.increment
      end

      expect(delivered_count.value).to eq length
    end
  end
end
