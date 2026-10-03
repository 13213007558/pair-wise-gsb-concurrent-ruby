require 'concurrent/executor/fork_join_executor'
require 'concurrent/atomic/atomic_fixnum'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/cyclic_barrier'

module Concurrent

  RSpec.describe ForkJoinExecutor do

    let(:pool) { ForkJoinExecutor.new(max_threads: 2) }

    after do
      pool.shutdown
      pool.wait_for_termination(10)
    end

    context 'recursive divide and conquer' do

      it 'completes on a single worker and runs every task exactly once' do
        single = ForkJoinExecutor.new(max_threads: 1)
        begin
          executions = AtomicFixnum.new(0)
          sum = nil
          sum = ->(lo, hi) do
            executions.increment
            if hi - lo <= 8
              (lo...hi).reduce(0, :+)
            else
              mid   = (lo + hi) / 2
              left  = single.fork { sum.call(lo, mid) }
              right = single.fork { sum.call(mid, hi) }
              left.join + right.join
            end
          end

          result = single.fork { sum.call(0, 64) }.join

          expect(result).to eq (0...64).reduce(0, :+)
          # 8 leaves + 7 internal nodes: each forked task ran exactly once.
          expect(executions.value).to eq 15
        ensure
          single.shutdown
          single.wait_for_termination(10)
        end
      end

      it 'lets external callers join normally' do
        task = pool.fork { 40 + 2 }
        expect(task.join).to eq 42
        expect(task).to be_done
        expect(task.value).to eq 42
        expect(task.reason).to be_nil
      end
    end

    context 'when every worker is blocked in join' do

      it 'workers help execute queued subtasks instead of deadlocking' do
        entered = CyclicBarrier.new(2) # both parent tasks are running
        joining = CyclicBarrier.new(2) # both parents are about to join
        child_executions = AtomicFixnum.new(0)

        parents = 2.times.map do |i|
          pool.fork do
            entered.wait(10)
            child = pool.fork do
              child_executions.increment
              i
            end
            # pin the moment where all workers are (about to be) in join
            # while both children are still queued
            joining.wait(10)
            child.join * 10
          end
        end

        expect(parents.map(&:join).sort).to eq [0, 10]
        expect(child_executions.value).to eq 2
      end
    end

    context 'nested forking and merging' do

      it 'merges the results of multiple joined subtasks' do
        root = pool.fork do
          children = 5.times.map { |i| pool.fork { i * i } }
          children.map(&:join).reduce(0, :+)
        end

        expect(root.join).to eq 30
      end

      it 'supports arbitrarily nested fork/join trees' do
        nested = nil
        nested = ->(depth) do
          if depth.zero?
            1
          else
            tasks = 2.times.map { pool.fork { nested.call(depth - 1) } }
            tasks.map(&:join).reduce(0, :+)
          end
        end

        expect(pool.fork { nested.call(4) }.join).to eq 16
      end
    end

    context 'exception propagation' do

      it 're-raises the task exception on join' do
        task = pool.fork { raise ArgumentError, 'boom' }

        expect { task.join }.to raise_error(ArgumentError, 'boom')
        expect(task).to be_done
        expect(task.reason).to be_a(ArgumentError)
        expect(task.value).to be_nil
      end

      it 'propagates exceptions through nested joins' do
        outer = pool.fork do
          inner = pool.fork { raise RuntimeError, 'inner failure' }
          inner.join
        end

        expect { outer.join }.to raise_error(RuntimeError, 'inner failure')
        expect(outer.reason).to be_a(RuntimeError)
      end
    end

    context 'dependency cycles' do

      it 'detects workers mutually waiting on each other' do
        both_running = CyclicBarrier.new(2)
        assigned = CountDownLatch.new(1)
        first = second = nil
        first  = pool.fork { both_running.wait(10); assigned.wait(10); second.join }
        second = pool.fork { both_running.wait(10); assigned.wait(10); first.join }
        assigned.count_down

        expect { first.join }.to raise_error(ForkJoinExecutor::CyclicDependencyError)
        expect { second.join }.to raise_error(ForkJoinExecutor::CyclicDependencyError)
      end

      it 'detects a task joining itself' do
        assigned = CountDownLatch.new(1)
        task = nil
        task = pool.fork { assigned.wait(10); task.join }
        assigned.count_down

        expect { task.join }.to raise_error(ForkJoinExecutor::CyclicDependencyError)
      end
    end

    context 'shutdown' do

      it 'fails queued tasks so their joins get a definite outcome' do
        single = ForkJoinExecutor.new(max_threads: 1)
        started = CountDownLatch.new(1)
        gate    = CountDownLatch.new(1)
        ran     = AtomicFixnum.new(0)

        running = single.fork do
          started.count_down
          gate.wait(10)
          :ran
        end
        queued = 3.times.map { single.fork { ran.increment } }

        started.wait(10)
        single.shutdown

        queued.each do |task|
          expect { task.join }.to raise_error(ForkJoinExecutor::ShutdownError)
          expect(task).to be_done
        end
        expect(ran.value).to eq 0
        expect { single.fork { :never } }.to raise_error(RejectedExecutionError)

        gate.count_down
        expect(running.join).to eq :ran
        expect(single.wait_for_termination(10)).to be true
        expect(single).to be_shutdown
      end
    end

    context 'worker limit' do

      it 'never runs more tasks concurrently than max_threads' do
        capped  = ForkJoinExecutor.new(max_threads: 3)
        barrier = CyclicBarrier.new(3)
        current = AtomicFixnum.new(0)
        peak    = AtomicFixnum.new(0)

        begin
          tasks = 9.times.map do
            capped.fork do
              now = current.increment
              peak.update { |max| [max, now].max }
              barrier.wait(10)
              current.decrement
              :ok
            end
          end

          expect(tasks.map(&:join)).to all(eq(:ok))
          expect(peak.value).to eq 3
          expect(capped.max_threads).to eq 3
        ensure
          capped.shutdown
          capped.wait_for_termination(10)
        end
      end
    end
  end
end
