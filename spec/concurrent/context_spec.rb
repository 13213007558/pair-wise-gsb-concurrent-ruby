require 'concurrent'
require 'concurrent/context'
require 'concurrent/future'
require 'concurrent/promises'
require 'concurrent/executor/fixed_thread_pool'
require 'concurrent/executor/cached_thread_pool'
require 'concurrent/executor/single_thread_executor'
require 'concurrent/executor/simple_executor_service'
require 'concurrent/executor/immediate_executor'
require 'concurrent/executor/indirect_immediate_executor'
require 'concurrent/executor/serialized_execution_delegator'

module Concurrent
  RSpec.describe Context do
    # Each example gets a freshly registered variable. Registry state of one
    # variable never affects another because slots are independent.
    let!(:var) { ContextVar.new(:trace_id) }

    after do
      # Reset the current thread so tests never observe values set inside
      # examples through the per-thread storage.
      Context.registered.each(&:clear)
    end

    describe ContextVar do
      it 'is registered for propagation' do
        expect(Context.registered).to include(var)
      end

      it 'does not leak registry state between distinct variables' do
        a = ContextVar.new(:a)
        b = ContextVar.new(:b)
        expect(Context.registered).to include(a, b, var)
      end

      it 'returns the default when unset' do
        v = ContextVar.new(:d, default: 7)
        expect(v.value).to eq 7
      end

      it 'evaluates the default block lazily per thread' do
        counter = Concurrent::AtomicFixnum.new(0)
        v = ContextVar.new(:db) { counter.increment }
        expect(v.value).to eq 1
        expect(v.value).to eq 1
        values = Queue.new
        Thread.new { values << v.value }.join
        expect(values.pop).to eq 2
      end

      it 'distinguishes an explicitly set nil from unset' do
        expect(var.set?).to be false
        expect(var.value).to be_nil
        var.value = nil
        expect(var.set?).to be true
        expect(var.value).to be_nil
        var.clear
        expect(var.set?).to be false
      end

      it 'rejects using both default and default_block' do
        expect { ContextVar.new(:x, default: 1, default_block: -> { 2 }) }.
          to raise_error(ArgumentError)
      end

      it 'rejects an unknown copy strategy' do
        expect { ContextVar.new(:x, copy: :wat) }.to raise_error(ArgumentError)
      end

      it 'binds a value for the block only' do
        var.bind('a') do
          expect(var.value).to eq 'a'
        end
        expect(var.set?).to be false
        var.value = 'outer'
        var.bind('inner') do
          expect(var.value).to eq 'inner'
        end
        expect(var.value).to eq 'outer'
      end

      describe ':copy option' do
        it ':none (default) shares the object' do
          v = ContextVar.new(:n)
          v.value = +'abc'
          snapshot = Context.capture
          v.value << 'd'
          expect(snapshot[v]).to eq 'abcd'
        end

        it ':dup duplicates on capture' do
          v = ContextVar.new(:d, copy: :dup)
          v.value = +'abc'
          snapshot = Context.capture
          v.value << 'd'
          expect(snapshot[v]).to eq 'abc'
        end

        it ':clone duplicates on capture' do
          v = ContextVar.new(:c, copy: :clone)
          v.value = +'abc'
          snapshot = Context.capture
          v.value << 'd'
          expect(snapshot[v]).to eq 'abc'
        end

        it ':marshal deep copies on capture' do
          v = ContextVar.new(:m, copy: :marshal)
          v.value = { a: [1] }
          snapshot = Context.capture
          snapshot[v][:a] << 2
          expect(v.value).to eq(a: [1])
        end

        it 'accepts a callable copier' do
          v = ContextVar.new(:cu, copy: ->(x) { x + 1 })
          v.value = 1
          expect(Context.capture[v]).to eq 2
        end
      end
    end

    describe '.capture and .activate' do
      it 'captures all registered values' do
        var.value = 'a'
        other = ContextVar.new(:other)
        other.value = 'b'
        snapshot = Context.capture
        expect(snapshot[var]).to eq 'a'
        expect(snapshot[other]).to eq 'b'
      end

      it 'installs the snapshot and restores the thread afterwards' do
        var.value = 'outer'
        snapshot = Context.capture
        var.value = 'changed'
        Context.activate(snapshot) { expect(var.value).to eq 'outer' }
        expect(var.value).to eq 'changed'
      end

      it 'restores even when the block raises' do
        var.value = 'outer'
        snapshot = Context.capture
        expect {
          Context.activate(snapshot) { raise 'boom' }
        }.to raise_error('boom')
        expect(var.value).to eq 'outer'
      end

      it 'an empty snapshot isolates and leaves no residue' do
        var.value = 'outer'
        Context.activate(Context.empty) do
          expect(var.value).to be_nil
          var.value = 'inside'
        end
        expect(var.value).to eq 'outer'
      end
    end

    shared_examples 'context propagating executor' do
      it 'propagates registered variables captured at submission' do
        var.value = 'request-A'
        result = Queue.new
        executor.post { result << var.value }
        expect(result.pop).to eq 'request-A'
      end

      it 'does not propagate unrelated thread variables' do
        # A purely inline executor runs on the calling thread, where an
        # ordinary thread local is legitimately visible; this assertion only
        # applies to executors which switch threads.
        skip 'the task runs on the calling thread' if executor.class == Concurrent::ImmediateExecutor
        tlv = ThreadLocalVar.new(:private)
        tlv.value = 'secret'
        result = Queue.new
        executor.post { result << tlv.value }
        expect(result.pop).to eq :private
      end

      it 'restores the worker value after the task' do
        var.value = 'request-A'
        done = Queue.new
        executor.post { done << :ok }
        done.pop
        var.value = nil
        executor.post { done << var.value }
        expect(done.pop).to be_nil
      end

      it 'keeps processing tasks after a task that raises' do
        if executor.is_a?(Concurrent::ImmediateExecutor) &&
           !executor.is_a?(Concurrent::IndirectImmediateExecutor)
          var.value = 'before'
          expect { executor.post { raise 'boom' } }.to raise_error('boom')
          expect(var.value).to eq 'before'
        else
          # Threaded executors swallow the exception but must still restore
          # the worker and accept further work. A dedicated single-thread
          # executor guarantees the verification task runs only after the
          # raising task has unwound (and restored the worker).
          pool = Concurrent::SingleThreadExecutor.new
          raised = Queue.new
          var.value = 'before'
          pool.post do
            begin
              raise 'boom'
            ensure
              raised << :raised
            end
          end
          raised.pop
          var.value = nil
          seen = Queue.new
          pool.post { seen << var.value }
          expect(seen.pop).to be_nil
          pool.shutdown
          pool.wait_for_termination
        end
      end
    end

    describe 'thread pool executors' do
      let(:executor) { FixedThreadPool.new(2) }
      after { executor.shutdown; executor.wait_for_termination }
      include_examples 'context propagating executor'

      it 'leaves no trace id on a worker reused for a second request' do
        pool = FixedThreadPool.new(1)
        seen = Queue.new
        var.value = 'request-1'
        pool.post { seen << var.value }
        expect(seen.pop).to eq 'request-1'
        var.value = 'request-2'
        pool.post { seen << var.value }
        expect(seen.pop).to eq 'request-2'
        var.value = nil
        pool.post { seen << var.value }
        expect(seen.pop).to be_nil
        pool.shutdown
        pool.wait_for_termination
      end

      it 'keeps concurrent requests isolated on shared workers' do
        pool = CachedThreadPool.new
        gate = Concurrent::CountDownLatch.new(2)
        release = Concurrent::CountDownLatch.new(1)
        seen = Concurrent::Array.new
        2.times do |i|
          var.bind("request-#{i}") do
            pool.post do
              gate.count_down
              release.wait(2)
              seen << var.value
            end
          end
        end
        gate.wait(2)
        release.count_down
        sleep 0.1
        pool.shutdown
        pool.wait_for_termination
        expect(seen.sort).to eq ['request-0', 'request-1']
      end
    end

    describe ImmediateExecutor do
      let(:executor) { ImmediateExecutor.new }
      include_examples 'context propagating executor'

      it 'captures and restores inline, including on rejection-like stop' do
        var.value = 'a'
        executor.post { var.value = 'b' }
        expect(var.value).to eq 'a'
        executor.shutdown
        expect(executor.post { :noop }).to be false
      end
    end

    describe IndirectImmediateExecutor do
      let(:executor) { IndirectImmediateExecutor.new }
      after { executor.shutdown; executor.wait_for_termination }
      include_examples 'context propagating executor'
    end

    describe SimpleExecutorService do
      let(:executor) { SimpleExecutorService.new }
      after { executor.shutdown; executor.wait_for_termination }
      include_examples 'context propagating executor'
    end

    describe SerializedExecutionDelegator do
      let(:executor) { SerializedExecutionDelegator.new(FixedThreadPool.new(1)) }
      after { executor.shutdown; executor.wait_for_termination }
      include_examples 'context propagating executor'

      it 'uses the submitting context for a stashed job' do
        pool = SerializedExecutionDelegator.new(FixedThreadPool.new(1))
        hold = Concurrent::CountDownLatch.new(1)
        go   = Concurrent::CountDownLatch.new(1)
        seen = Queue.new
        var.value = 'first'
        pool.post { hold.wait(2); go.count_down }
        var.value = 'second'
        pool.post { seen << var.value }
        hold.count_down
        expect(seen.pop).to eq 'second'
        pool.shutdown
        pool.wait_for_termination
      end
    end

    describe 'rejected execution' do
      it ':abort does not touch the submitting thread' do
        pool = FixedThreadPool.new(1)
        pool.shutdown
        pool.wait_for_termination
        var.value = 'a'
        expect { pool.post { var.value = 'b' } }.to raise_error(RejectedExecutionError)
        expect(var.value).to eq 'a'
      end

      it ':caller_runs fallback restores the calling thread' do
        pool = FixedThreadPool.new(1, max_queue: 1, fallback_policy: :caller_runs)
        pool.shutdown
        pool.wait_for_termination
        var.value = 'a'
        ran = Concurrent::AtomicBoolean.new(false)
        pool.post { ran.make_true; var.value = 'b' }
        expect(ran.true?).to be true
        expect(var.value).to eq 'a'
      end

      it ':discard leaves no residue' do
        pool = FixedThreadPool.new(1, fallback_policy: :discard)
        pool.shutdown
        pool.wait_for_termination
        var.value = 'a'
        expect(pool.post { var.value = 'b' }).to be false
        expect(var.value).to eq 'a'
      end
    end

    describe 'legacy Future' do
      it 'captures at submission by default through the executor' do
        var.value = 'a'
        future = Future.execute(executor: :immediate) { var.value }
        expect(future.value).to eq 'a'
      end

      it 'isolates with context: false' do
        var.value = 'a'
        future = Future.new(executor: :immediate, context: false) { var.value }.execute
        expect(future.value).to be_nil
      end

      it 'captures at execute time with context: true' do
        var.value = 'a'
        future = Future.new(context: true) { var.value }.execute
        future.wait(2)
        expect(future.value).to eq 'a'
      end
    end

    describe Promises do
      describe 'root futures' do
        it 'capture the submitting context' do
          var.value = 'a'
          expect(Promises.future { var.value }.value!(2)).to eq 'a'
        end
      end

      describe '#then' do
        it 'is isolated by default' do
          var.value = 'a'
          expect(Promises.future { 1 }.then { var.value }.value!(2)).to be_nil
        end

        it 'propagates with context: true' do
          var.value = 'a'
          expect(Promises.future { 1 }.then(context: true) { var.value }.value!(2)).to eq 'a'
        end

        it 'captures at registration, not when the blocker resolves' do
          var.value = 'at-registration'
          root = Promises.resolvable_future
          chained = root.then(context: true) { var.value }
          var.value = 'at-resolution'
          root.fulfill(nil)
          expect(chained.value!(2)).to eq 'at-registration'
        end
      end

      describe '#rescue and #chain' do
        it 'rescue propagates with context: true' do
          var.value = 'a'
          value = Promises.future { raise 'x' }.rescue(context: true) { var.value }.value!(2)
          expect(value).to eq 'a'
        end

        it 'chain propagates with context: true' do
          var.value = 'a'
          expect(Promises.future { 1 }.chain(context: true) { var.value }.value!(2)).to eq 'a'
        end

        it 'chain is isolated by default' do
          var.value = 'a'
          expect(Promises.future { 1 }.chain { var.value }.value!(2)).to be_nil
        end
      end

      it 'accepts an explicit snapshot' do
        var.value = 'a'
        snapshot = Context.capture
        var.value = nil
        chained = Promises.future { 1 }.then(context: snapshot) { var.value }
        expect(chained.value!(2)).to eq 'a'
      end
    end

    describe 'many variables' do
      it 'keeps distinct variables isolated on every thread' do
        vars = 3.times.map { |i| ContextVar.new(:"var_#{i}") }
        vars.each_with_index { |v, i| v.value = i }
        snapshot = Context.capture
        vars.each_with_index { |v, i| expect(snapshot[v]).to eq i }

        # A worker sees the captured values and leaves nothing behind after.
        pool = Concurrent::FixedThreadPool.new(1)
        seen = Queue.new
        pool.post { vars.each { |v| seen << v.value } }
        expect(3.times.map { seen.pop }).to eq [0, 1, 2]
        vars.each(&:clear)
        pool.post { seen << vars.map(&:set?) }
        expect(seen.pop).to eq [false, false, false]
        pool.shutdown
        pool.wait_for_termination
      end
    end
  end
end
