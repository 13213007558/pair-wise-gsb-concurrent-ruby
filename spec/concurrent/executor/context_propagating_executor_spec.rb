require 'concurrent/executor/context_propagating_executor'
require 'concurrent/executor/fixed_thread_pool'
require 'concurrent/executor/single_thread_executor'
require 'concurrent/executor/immediate_executor'
require 'concurrent/executor/thread_pool_executor'
require 'concurrent/atomic/count_down_latch'

module Concurrent
  RSpec.describe ContextPropagatingExecutor do
    let(:trace_id) { ContextVar.new(:"trace_#{object_id}") }
    let(:secret)   { ContextVar.new(:"secret_#{object_id}") }

    after do
      [trace_id, secret].each { |v| v.__set_raw_value__(Context::UNSET) }
    end

    subject(:executor) { described_class.new(FixedThreadPool.new(1)) }

    after do
      pool = executor.instance_variable_get(:@executor)
      pool.shutdown if pool.respond_to?(:shutdown) && pool.running?
      pool.wait_for_termination if pool.respond_to?(:wait_for_termination)
    end

    it { is_expected.to respond_to(:post) }
    it { is_expected.to respond_to(:can_overflow?) }
    it { is_expected.to respond_to(:serialized?) }

    it 'exposes the wrapped executor' do
      expect(executor.executor).to be_a(FixedThreadPool)
    end

    it 'requires a block' do
      expect { executor.post }.to raise_error(ArgumentError)
    end

    it 'delegates #can_overflow? and #serialized?' do
      inner = double('executor', can_overflow?: :ovf, serialized?: :ser)
      executor = described_class.new(inner)
      expect(executor.can_overflow?).to eq :ovf
      expect(executor.serialized?).to eq :ser
    end

    context 'propagation' do
      let(:pool) { FixedThreadPool.new(2) }

      after { pool.shutdown; pool.wait_for_termination }

      it 'runs the task with the submitting thread context installed' do
        executor = described_class.new(pool)
        trace_id.value = 'abc'
        latch = CountDownLatch.new(1)
        seen = nil
        executor.post do
          seen = trace_id.value
          latch.count_down
        end
        latch.wait
        expect(seen).to eq 'abc'
      end

      it 'captures at submission time, not at execution time' do
        executor = described_class.new(pool)
        block_started = CountDownLatch.new(1)
        unblock = CountDownLatch.new(1)
        finish = CountDownLatch.new(1)
        seen = nil

        trace_id.value = 'first'
        executor.post do
          block_started.count_down
          unblock.wait
          seen = trace_id.value
          finish.count_down
        end
        block_started.wait
        trace_id.value = 'second'
        unblock.count_down
        finish.wait
        expect(seen).to eq 'first'
      end

      it 'never leaks task-set values back onto the submitting thread' do
        executor = described_class.new(pool)
        done = CountDownLatch.new(1)
        trace_id.value = 'main'
        executor.post do
          trace_id.value = 'worker'
          done.count_down
        end
        done.wait
        expect(trace_id.value).to eq 'main'
      end

      it 'does not propagate variables which are not set on submitter' do
        executor = described_class.new(pool)
        done = CountDownLatch.new(1)
        seen = :not_nil
        executor.post do
          seen = trace_id.value
          done.count_down
        end
        done.wait
        expect(seen).to be_nil
      end

      it 'passes arguments through to the task' do
        executor = described_class.new(pool)
        done = CountDownLatch.new(1)
        received = nil
        executor.post(1, 2, 3) do |*args|
          received = args
          done.count_down
        end
        done.wait
        expect(received).to eq [1, 2, 3]
      end
    end

    context 'worker reuse' do
      it 'restores worker bindings between two unrelated requests' do
        single = SingleThreadExecutor.new
        executor = described_class.new(single)
        first_done = CountDownLatch.new(1)
        run_second = CountDownLatch.new(1)
        second_done = CountDownLatch.new(1)
        second_seen = :init

        trace_id.value = 'request-1'
        executor.post do
          first_done.count_down
          run_second.wait
        end
        first_done.wait

        trace_id.__set_raw_value__(Context::UNSET)
        executor.post do
          second_seen = trace_id.value
          second_done.count_down
        end
        run_second.count_down
        second_done.wait

        expect(second_seen).to be_nil
        single.shutdown
        single.wait_for_termination
      end
    end

    context 'cleanup guarantees' do
      it 'restores after the task raises' do
        single = SingleThreadExecutor.new
        executor = described_class.new(single)

        raised = CountDownLatch.new(1)
        trace_id.value = 'boom'
        executor.post do
          raise 'boom'
        rescue RuntimeError
          raised.count_down
        end
        raised.wait

        seen = :init
        done = CountDownLatch.new(1)
        fresh = ContextVar.new(:"fresh_#{object_id}")
        fresh.value = 'ok'
        executor.post do
          seen = fresh.value
          done.count_down
        end
        done.wait
        expect(seen).to eq 'ok'
        fresh.__set_raw_value__(Context::UNSET)
        single.shutdown
        single.wait_for_termination
      end

      it 'restores the submitting thread when an inline executor runs the task' do
        immediate = ImmediateExecutor.new
        executor = described_class.new(immediate)
        trace_id.value = 'before'
        executor.post { trace_id.value = 'inside' }
        expect(trace_id.value).to eq 'before'
      end

      it 'restores the submitting thread even when the inline task raises' do
        immediate = ImmediateExecutor.new
        executor = described_class.new(immediate)
        trace_id.value = 'before'
        expect do
          executor.post { raise 'boom' }
        end.to raise_error('boom')
        expect(trace_id.value).to eq 'before'
      end

      it 'restores the caller when the pool fallback runs the task inline' do
        pool = ThreadPoolExecutor.new(
          min_threads: 1, max_threads: 1, max_queue: 1,
          fallback_policy: :caller_runs
        )
        executor = described_class.new(pool)

        started = CountDownLatch.new(1)
        release = CountDownLatch.new(1)
        executor.post { started.count_down; release.wait }
        started.wait
        executor.post {} # fills the queue
        trace_id.value = 'caller'
        executor.post {} # rejected -> caller_runs on this thread
        expect(trace_id.value).to eq 'caller'
        release.count_down
        pool.shutdown
        pool.wait_for_termination
      end

      it 'does not touch the submitting thread when the task is discarded' do
        pool = ThreadPoolExecutor.new(
          min_threads: 1, max_threads: 1, max_queue: 1,
          fallback_policy: :discard
        )
        executor = described_class.new(pool)

        started = CountDownLatch.new(1)
        release = CountDownLatch.new(1)
        executor.post { started.count_down; release.wait }
        started.wait
        executor.post {} # fills the queue
        trace_id.value = 'caller'
        result = executor.post {} # rejected -> discarded

        expect(result).to be false
        expect(trace_id.value).to eq 'caller'
        release.count_down
        pool.shutdown
        pool.wait_for_termination
      end
    end

    context 'fixed snapshot' do
      it 'uses the given snapshot for every task' do
        fixed_pool = FixedThreadPool.new(2)
        trace_id.value = 'fixed'
        snapshot = Context.capture
        fixed_executor = described_class.new(fixed_pool, snapshot: snapshot)
        trace_id.value = 'changed-after'

        values = []
        3.times do
          done = CountDownLatch.new(1)
          fixed_executor.post do
            values << trace_id.value
            done.count_down
          end
          done.wait
        end
        expect(values).to eq ['fixed', 'fixed', 'fixed']
        fixed_pool.shutdown
        fixed_pool.wait_for_termination
      end
    end
  end
end
