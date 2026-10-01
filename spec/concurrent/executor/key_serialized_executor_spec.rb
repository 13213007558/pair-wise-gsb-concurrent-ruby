require 'concurrent'
require 'concurrent/executor/key_serialized_executor'
require 'timeout'
require 'weakref'

RSpec.describe Concurrent::KeySerializedExecutor do

  let(:executor) { described_class.new(2) }

  after(:each) do
    executor.kill
    executor.wait_for_termination(5)
  end

  describe '#initialize' do
    it 'raises when the number of threads is not positive' do
      expect { described_class.new(0) }.to raise_error(ArgumentError)
      expect { described_class.new(-1) }.to raise_error(ArgumentError)
    end

    it 'raises on unknown fallback policy' do
      expect { described_class.new(2, fallback_policy: :bogus) }.to raise_error(ArgumentError)
    end

    it 'raises on negative max_tasks' do
      expect { described_class.new(2, max_tasks: -1) }.to raise_error(ArgumentError)
    end

    it 'is running when created' do
      expect(executor).to be_running
      expect(executor).not_to be_shutdown
      expect(executor).not_to be_serialized
    end

    it 'exposes the configuration' do
      ex = described_class.new(3, max_tasks: 42, fallback_policy: :discard)
      expect(ex.max_threads).to eq 3
      expect(ex.max_tasks).to eq 42
      expect(ex.fallback_policy).to eq :discard
    end
  end

  describe '#post' do
    it 'raises ArgumentError without a block' do
      expect { executor.post(:key) }.to raise_error(ArgumentError)
    end

    it 'returns true when the task is queued' do
      expect(executor.post(:key) { nil }).to be true
    end

    it 'passes arguments to the task' do
      latch = Concurrent::CountDownLatch.new(1)
      executor.post(:key, 1, 2, 3) do |a, b, c|
        expect([a, b, c]).to eq [1, 2, 3]
        latch.count_down
      end
      expect(latch.wait(5)).to be true
    end

    it 'executes tasks of the same key in submission order' do
      done = Concurrent::CountDownLatch.new(50)
      seen = Concurrent::Array.new
      mutex = Mutex.new
      50.times do |i|
        executor.post(:account) do
          sleep(rand * 0.002)
          mutex.synchronize { seen << i }
          done.count_down
        end
      end
      expect(done.wait(10)).to be true
      expect(seen.to_a).to eq (0...50).to_a
    end

    it 'never runs two tasks of the same key simultaneously' do
      concurrent = Concurrent::AtomicBoolean.new(false)
      overlapped = Concurrent::AtomicBoolean.new(false)
      done = Concurrent::CountDownLatch.new(100)

      100.times do
        executor.post(:account) do
          if concurrent.true?
            overlapped.make_true
          else
            concurrent.make_true
            sleep 0.001
            concurrent.make_false
          end
          done.count_down
        end
      end

      expect(done.wait(10)).to be true
      expect(overlapped.true?).to be false
    end

    it 'runs tasks of different keys in parallel' do
      started = Concurrent::CountDownLatch.new(2)
      release = Concurrent::CountDownLatch.new(1)
      finished = Concurrent::CountDownLatch.new(2)

      2.times do |i|
        executor.post("account-#{i}") do
          started.count_down
          release.wait(5)
          finished.count_down
        end
      end

      expect(started.wait(5)).to be true
      release.count_down
      expect(finished.wait(5)).to be true
    end

    it 'serializes equal keys even when they are different objects' do
      k1 = 'account'
      k2 = 'account'
      expect(k1).not_to be(k2)

      concurrent = Concurrent::AtomicBoolean.new(false)
      overlapped = Concurrent::AtomicBoolean.new(false)
      done = Concurrent::CountDownLatch.new(2)

      [k1, k2].each do |key|
        executor.post(key) do
          if concurrent.true?
            overlapped.make_true
          else
            concurrent.make_true
            sleep 0.01
            concurrent.make_false
          end
          done.count_down
        end
      end

      expect(done.wait(5)).to be true
      expect(overlapped.true?).to be false
    end

    it 'appends a task posted from within a task after already queued tasks' do
      single = described_class.new(1, max_tasks: 0)
      seen = Concurrent::Array.new
      done = Concurrent::CountDownLatch.new(5)
      single.post(:k) do
        seen << 1
        done.count_down
        single.post(:k) { seen << 5; done.count_down }
      end
      single.post(:k) { seen << 2; done.count_down }
      single.post(:k) { seen << 3; done.count_down }
      single.post(:k) { seen << 4; done.count_down }
      expect(done.wait(5)).to be true
      expect(seen.to_a).to eq [1, 2, 3, 4, 5]
      single.shutdown
      expect(single.wait_for_termination(5)).to be true
    end

    it 'continues with later tasks of a key after a task raises' do
      done = Concurrent::CountDownLatch.new(1)
      executor.post(:boom) { raise 'expected failure' }
      executor.post(:boom) { done.count_down }
      expect(done.wait(5)).to be true
    end

    it 'does not let a busy key block progress of another key' do
      # 40 tasks on :busy which each sleep a bit; one task on :free must finish
      # well before the busy key drains, proving the worker is released after
      # every task.
      free_done = Concurrent::CountDownLatch.new(1)
      all_done = Concurrent::CountDownLatch.new(41)

      40.times do
        executor.post(:busy) do
          sleep 0.01
          all_done.count_down
        end
      end
      executor.post(:free) do
        free_done.count_down
        all_done.count_down
      end

      expect(free_done.wait(5)).to be true
      expect(all_done.wait(10)).to be true
    end
  end

  describe 'task limit' do
    it 'bounds the number of accepted-but-not-completed tasks' do
      # 2 workers, 5 accepted tasks max. Block the two running tasks and fill
      # the remaining capacity; further posts are rejected.
      started = Concurrent::CountDownLatch.new(2)
      release = Concurrent::CountDownLatch.new(1)
      limited = described_class.new(2, max_tasks: 5, fallback_policy: :abort)

      limited.post(:a) { started.count_down; release.wait(5) }
      limited.post(:b) { started.count_down; release.wait(5) }
      expect(started.wait(5)).to be true
      expect(limited.post(:a) { nil }).to be true
      expect(limited.post(:b) { nil }).to be true
      expect(limited.post(:c) { nil }).to be true

      expect(limited.scheduled_task_count).to eq 5
      expect { limited.post(:a) { nil } }.to raise_error(Concurrent::RejectedExecutionError)
      expect(limited.scheduled_task_count).to eq 5

      release.count_down
      limited.shutdown
      expect(limited.wait_for_termination(5)).to be true
    end

    it 'counts pending tasks across all keys' do
      release = Concurrent::CountDownLatch.new(1)
      started = Concurrent::CountDownLatch.new(2)
      limited = described_class.new(2, max_tasks: 4, fallback_policy: :abort)

      limited.post(:a) { started.count_down; release.wait(5) }
      limited.post(:b) { started.count_down; release.wait(5) }
      expect(started.wait(5)).to be true
      limited.post(:a) { nil }
      limited.post(:b) { nil }
      expect { limited.post(:c) { nil } }.to raise_error(Concurrent::RejectedExecutionError)

      release.count_down
      limited.shutdown
      expect(limited.wait_for_termination(5)).to be true
    end

    context 'with :discard policy' do
      it 'returns false and does not enqueue the task' do
        release = Concurrent::CountDownLatch.new(1)
        limited = described_class.new(1, max_tasks: 1, fallback_policy: :discard)
        limited.post(:a) { release.wait(5) }
        sleep 0.05 # make sure the first task is running
        expect(limited.post(:a) { nil }).to be false
        expect(limited.scheduled_task_count).to eq 1
        release.count_down
        limited.shutdown
        expect(limited.wait_for_termination(5)).to be true
      end
    end

    context 'with :caller_runs policy' do
      it 'executes a saturated task on the calling thread in key order' do
        order = Concurrent::Array.new
        release = Concurrent::CountDownLatch.new(1)
        limited = described_class.new(1, max_tasks: 1, fallback_policy: :caller_runs)

        limited.post(:a) { order << :worker; release.wait(5) }
        sleep 0.05

        ran_on = Concurrent::AtomicReference.new(nil)
        t = Thread.new do
          limited.post(:a) do
            ran_on.set(Thread.current)
            order << :caller
          end
        end

        sleep 0.1
        expect(order.to_a).to eq [:worker]
        expect(ran_on.value).to be_nil
        release.count_down
        t.join(5)
        expect(ran_on.value).to be(t)
        expect(order.to_a).to eq [:worker, :caller]

        limited.shutdown
        expect(limited.wait_for_termination(5)).to be true
      end

      it 'runs re-entrant posts from within a task inline without deadlock' do
        done = Concurrent::CountDownLatch.new(1)
        limited = described_class.new(1, max_tasks: 1, fallback_policy: :caller_runs)
        nested_ran = Concurrent::AtomicBoolean.new(false)

        limited.post(:a) do
          limited.post(:a) { nested_ran.make_true }
          done.count_down
        end

        expect(done.wait(5)).to be true
        expect(nested_ran.true?).to be true
        limited.shutdown
        expect(limited.wait_for_termination(5)).to be true
      end
    end
  end

  describe 'idle key reclamation' do
    it 'removes keys with no pending work' do
      done = Concurrent::CountDownLatch.new(2)
      executor.post(:a) { done.count_down }
      executor.post(Object.new) { done.count_down }
      expect(done.wait(5)).to be true

      wait_for_event(5) { executor.key_count.zero? }
      expect(executor.key_count).to eq 0
    end

    it 'allows an idle key to be garbage collected' do
      weak = post_with_ephemeral_key(executor)
      GC.start
      wait_for_event(5) { !weak.weakref_alive? }
      expect(weak.weakref_alive?).to be_falsey
    end
  end

  describe '#shutdown' do
    it 'finishes already accepted tasks in per-key order and then terminates' do
      seen = Concurrent::Array.new
      20.times { |i| executor.post(:a) { sleep 0.001; seen << i } }
      executor.shutdown
      expect(executor.wait_for_termination(5)).to be true
      expect(seen.to_a).to eq (0...20).to_a
      expect(executor).to be_shutdown
      expect(executor.scheduled_task_count).to eq 0
    end

    it 'does not accept new tasks after shutdown' do
      executor.shutdown
      expect(executor.wait_for_termination(5)).to be true
      expect { executor.post(:a) { nil } }.to raise_error(Concurrent::RejectedExecutionError)
    end

    it 'is idempotent' do
      expect(executor.shutdown).to be true
      expect(executor.shutdown).to be true
      expect(executor.wait_for_termination(5)).to be true
    end

    it 'drains tasks posted after a saturated caller_runs ticket' do
      limited = described_class.new(1, max_tasks: 2, fallback_policy: :caller_runs)
      release = Concurrent::CountDownLatch.new(1)
      started = Concurrent::CountDownLatch.new(1)
      seen = Concurrent::Array.new
      all = Concurrent::CountDownLatch.new(3)

      limited.post(:a) { started.count_down; release.wait(5); seen << 1; all.count_down }
      expect(started.wait(5)).to be true
      limited.post(:a) { seen << 2; all.count_down }
      t = Thread.new { limited.post(:a) { seen << 3; all.count_down } }
      sleep 0.05
      release.count_down
      t.join(5)

      limited.shutdown
      expect(limited.wait_for_termination(5)).to be true
      expect(all.wait(1)).to be true
      expect(seen.to_a).to eq [1, 2, 3]
    end
  end

  describe '#kill' do
    it 'wakes callers blocked on a caller_runs post and returns false' do
      limited = described_class.new(1, max_tasks: 1, fallback_policy: :caller_runs)
      release = Concurrent::CountDownLatch.new(1)
      limited.post(:a) { release.wait(10) }
      sleep 0.05

      result = nil
      t = Thread.new { result = limited.post(:a) { nil } }
      sleep 0.1
      limited.kill
      t.join(5)
      expect(result).to be false
      release.count_down
      expect(limited.wait_for_termination(5)).to be true
    end

    it 'terminates without running merely queued tasks' do
      ran = Concurrent::AtomicBoolean.new(false)
      release = Concurrent::CountDownLatch.new(1)
      limited = described_class.new(1)
      limited.post(:a) { release.wait(10) }
      sleep 0.05
      limited.post(:a) { ran.make_true }
      limited.kill
      release.count_down
      expect(limited.wait_for_termination(5)).to be true
      expect(ran.true?).to be false
    end
  end

  # Posts a task with a mutable key owned by this method's local scope and
  # returns a WeakRef to it, so the example binding does not keep the key alive.
  def post_with_ephemeral_key(executor)
    key = +'ephemeral-account'
    done = Concurrent::CountDownLatch.new(1)
    executor.post(key) { done.count_down }
    raise 'task did not finish' unless done.wait(5)
    WeakRef.new(key)
  end

  def wait_for_event(timeout)
    deadline = Time.now + timeout
    loop do
      return if yield
      break if Time.now >= deadline
      sleep 0.02
    end
  end
end

RSpec.describe Concurrent::KeySerializedExecutor, :stress do
  it 'preserves per-key order without overlaps under heavy load', :stress do
    executor = described_class.new(4, max_tasks: 0)
    keys = (0...8).map { |i| "account-#{i}" }
    per_thread = 500
    total = keys.size * per_thread

    # shared per-key counters define the global submission order per key
    counters = {}
    state = {}
    keys.each do |k|
      counters[k] = Concurrent::AtomicFixnum.new(0)
      state[k] = { next_seq: 0, running: false, overlapped: false, total: 0 }
    end
    state_mutex = Mutex.new
    done = Concurrent::CountDownLatch.new(total)

    threads = Array.new(8) do
      Thread.new do
        per_thread.times do |i|
          key = keys[i % keys.size]
          seq = counters[key].increment - 1
          executor.post(key, seq) do |s|
            state_mutex.synchronize do
              state[key][:overlapped] = true if state[key][:running]
              expect(state[key][:next_seq]).to eq(s)
              state[key][:running] = true
              state[key][:total] += 1
            end
            sleep(rand * 0.0005)
            state_mutex.synchronize do
              state[key][:next_seq] = s + 1
              state[key][:running] = false
            end
            done.count_down
          end
        end
      end
    end
    threads.each(&:join)

    expect(done.wait(60)).to be true
    keys.each do |k|
      expect(state[k][:next_seq]).to eq(state[k][:total])
      expect(state[k][:overlapped]).to be false
    end

    executor.shutdown
    expect(executor.wait_for_termination(30)).to be true
  end
end
