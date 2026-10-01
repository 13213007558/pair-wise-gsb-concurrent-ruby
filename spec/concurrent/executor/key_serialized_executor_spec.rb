require 'concurrent'

module Concurrent

  RSpec.describe KeySerializedExecutor do

    after(:each) do
      @executor&.kill
      @executor&.wait_for_termination(2)
    end

    def new_executor(*args)
      (@executor = described_class.new(*args))
    end

    def wait_for(seconds = 2)
      deadline = Concurrent.monotonic_time + seconds
      loop do
        return true if yield
        raise 'timed out' if Concurrent.monotonic_time >= deadline
        sleep 0.01
      end
    end

    it 'requires at least one thread' do
      expect { described_class.new(0) }.to raise_error(ArgumentError)
      expect { described_class.new(-1) }.to raise_error(ArgumentError)
    end

    it 'rejects a negative max_queue' do
      expect { described_class.new(2, max_queue: -1) }.to raise_error(ArgumentError)
    end

    it 'rejects an unknown fallback policy' do
      expect { described_class.new(2, fallback_policy: :bogus) }.to raise_error(ArgumentError)
    end

    it 'raises ArgumentError from #post without a block' do
      executor = new_executor(2)
      expect { executor.post(:k) }.to raise_error(ArgumentError)
    end

    context 'serialization within a key' do

      it 'runs tasks for the same key strictly in submission order' do
        executor = new_executor(2)
        done = Queue.new
        50.times { |i| executor.post(:account) { done << i } }
        order = Array.new(50) { done.pop }
        expect(order).to eq (0...50).to_a
      end

      it 'never runs two tasks for the same key at the same time' do
        executor = new_executor(2)
        running = 0
        overlap = AtomicFixnum.new(0)
        mutex = Mutex.new
        latch = CountDownLatch.new(50)

        50.times do
          executor.post(:account) do
            mutex.synchronize do
              overlap.increment if running.positive?
              running += 1
            end
            sleep 0.001
            mutex.synchronize { running -= 1 }
            latch.count_down
          end
        end

        expect(latch.wait(5)).to be_truthy
        expect(overlap.value).to eq 0
      end
    end

    context 'parallelism across keys' do

      it 'runs tasks for different keys concurrently' do
        executor = new_executor(2)
        first_started = Event.new
        second_running = Event.new

        executor.post(:a) do
          first_started.set
          sleep 0.2
        end
        # make sure :a has claimed a worker before :b starts
        expect(first_started.wait(2)).to be_truthy
        executor.post(:b) { second_running.set }

        expect(second_running.wait(1)).to be_truthy
      end

      it 'does not allow one busy key to starve another key (two workers)' do
        # :a submits a long burst; :b, submitted afterwards, must still finish
        # before :a's final task.
        executor = new_executor(2)
        order = Queue.new
        latch = CountDownLatch.new(24)

        20.times { |i| executor.post(:a) { sleep 0.002; order << [:a, i]; latch.count_down } }
        4.times  { |i| executor.post(:b) { sleep 0.002; order << [:b, i]; latch.count_down } }

        expect(latch.wait(5)).to be_truthy
        completed = []
        completed << order.pop until order.empty?

        a_order = completed.select { |k, _| k == :a }.map(&:last)
        b_order = completed.select { |k, _| k == :b }.map(&:last)
        expect(a_order).to eq (0...20).to_a
        expect(b_order).to eq (0...4).to_a

        last_a = completed.rindex { |k, _| k == :a }
        expect(completed[0...last_a].any? { |k, _| k == :b }).to be_truthy
      end

      it 'runs at most num_threads keys in parallel' do
        executor = new_executor(2)
        active = 0
        max_active = AtomicFixnum.new(0)
        mutex = Mutex.new
        entered = CountDownLatch.new(2)
        release = Event.new

        4.times do |i|
          executor.post(i) do
            mutex.synchronize do
              active += 1
              max_active.update { |v| [v, active].max }
            end
            entered.count_down
            release.wait(2)
            mutex.synchronize { active -= 1 }
          end
        end

        # exactly two tasks enter before release; the other two must wait
        expect(entered.wait(2)).to be_truthy
        sleep 0.2
        expect(max_active.value).to eq 2
        release.set
        wait_for { executor.queue_length.zero? }
      end
    end

    context 'total queue bound and rejection' do

      it 'counts accepted tasks in queue_length' do
        executor = new_executor(1, max_queue: 100)
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        executor.post(:a) {}
        executor.post(:b) {}
        # one task running, two still queued behind it (single worker)
        expect(wait_for { executor.queue_length == 3 }).to be_truthy
        expect(executor.queue_length).to eq 3
        release.set
        wait_for { executor.queue_length.zero? }
      end

      it 'rejects with RejectedExecutionError under :abort when the total queue is full' do
        executor = new_executor(1, max_queue: 1, fallback_policy: :abort)
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        expect { executor.post(:b) { } }.to raise_error(RejectedExecutionError)
        release.set
      end

      it 'returns false under :discard when the total queue is full' do
        executor = new_executor(1, max_queue: 1, fallback_policy: :discard)
        release = Event.new
        started = Event.new
        accepted = executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        expect(accepted).to eq true
        expect(executor.post(:b) { }).to eq false
        release.set
      end

      it 'blocks the caller under :caller_runs until room is available' do
        executor = new_executor(1, max_queue: 1, fallback_policy: :caller_runs)
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy

        posted = Concurrent::AtomicBoolean.new(false)
        thread = Thread.new do
          executor.post(:b) { }
          posted.make_true
        end

        sleep 0.1
        expect(posted.true?).to be(false) # blocked: queue is full
        release.set
        expect(thread.join(2)).to be_truthy
        expect(posted.true?).to be(true)
      end

      it 'treats max_queue zero as unbounded' do
        executor = new_executor(1, max_queue: 0)
        expect(executor.can_overflow?).to be(false)
        100.times { |i| executor.post(i) { } }
        executor.shutdown
        expect(executor.wait_for_termination(5)).to be_truthy
        @executor = nil
      end
    end

    context 'under high contention' do

      it 'preserves per-key order and never overlaps a key with many producers', :stress do
        executor = new_executor(4, max_queue: 10_000)
        key_count = 8
        per_key = 200
        producers = 4

        mutex = Mutex.new
        in_flight = Hash.new(0)
        overlaps = AtomicFixnum.new(0)
        completions = Array.new(key_count) { [] }
        completion_mutex = Mutex.new
        done = CountDownLatch.new(key_count * per_key)

        threads = Array.new(producers) do |producer|
          Thread.new do
            (per_key / producers).times do |j|
              key_count.times do |k|
                i = producer * (per_key / producers) + j
                executor.post(k) do
                  mutex.synchronize do
                    overlaps.increment if in_flight[k].positive?
                    in_flight[k] += 1
                  end
                  completion_mutex.synchronize { completions[k] << i }
                  mutex.synchronize { in_flight[k] -= 1 }
                  done.count_down
                end
              end
            end
          end
        end
        threads.each(&:join)

        expect(done.wait(15)).to be_truthy
        expect(overlaps.value).to eq 0
        completions.each do |sequence|
          expect(sequence).to eq sequence.sort
        end
      end
    end

    context 'task failures' do

      it 'continues running later tasks for the same key after one task raises' do
        executor = new_executor(2)
        ran = Queue.new
        executor.post(:a) { raise 'boom' }
        executor.post(:a) { ran << 1 }
        executor.post(:a) { ran << 2 }
        expect(ran.pop).to eq 1
        expect(ran.pop).to eq 2
      end
    end

    context 'idle key reclamation' do

      it 'removes a key once its tasks have all completed' do
        executor = new_executor(2)
        latch = CountDownLatch.new(3)
        3.times { executor.post(:temp) { latch.count_down } }
        expect(latch.wait(2)).to be_truthy
        wait_for { executor.key_count.zero? }
      end

      it 'keeps independent counts per key' do
        executor = new_executor(1)
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        executor.post(:a) {}
        executor.post(:b) {}
        expect(wait_for { executor.key_count == 2 }).to be_truthy
        release.set
        wait_for { executor.key_count.zero? }
      end
    end

    context 'shutdown' do

      it 'finishes every accepted task on graceful shutdown' do
        executor = new_executor(1)
        completed = Queue.new
        # The single worker is blocked on the first task while the rest queue.
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        5.times { |i| executor.post(:a) { completed << i } }

        executor.shutdown
        sleep 0.05
        expect(executor.running?).to be(false)
        expect(executor.shutdown?).to be(false)

        # tasks already accepted still drain even though no new tasks accepted
        expect { executor.post(:a) {} }.to raise_error(RejectedExecutionError)
        release.set
        expect(executor.wait_for_termination(5)).to be_truthy
        expect(executor.shutdown?).to be(true)

        order = Array.new(5) { completed.pop }
        expect(order).to eq (0...5).to_a
        @executor = nil
      end

      it 'dismisses queued tasks on kill while letting in-progress tasks finish' do
        executor = new_executor(1)
        ran_after_kill = AtomicFixnum.new(0)
        release = Event.new
        started = Event.new
        executor.post(:a) { started.set; release.wait(2) }
        expect(started.wait(2)).to be_truthy
        5.times { executor.post(:a) { ran_after_kill.increment } }

        executor.kill
        executor.wait_for_termination(5)
        release.set
        sleep 0.1
        expect(ran_after_kill.value).to eq 0
        expect(executor.shutdown?).to be(true)
        @executor = nil
      end

      it 'rejects new posts after shutdown (caller_runs does not hang)' do
        executor = new_executor(2, fallback_policy: :caller_runs)
        executor.shutdown
        executor.wait_for_termination(5)
        expect { executor.post(:a) {} }.to_not raise_error
        @executor = nil
      end
    end
  end
end
