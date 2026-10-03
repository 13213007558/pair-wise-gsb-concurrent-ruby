require 'concurrent/executor/fork_join_executor'
require 'concurrent/atomic/atomic_fixnum'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/event'

module Concurrent

  RSpec.describe ForkJoinExecutor do

    let(:pool) { described_class.new(pool_size) }
    let(:pool_size) { 2 }

    after(:each) do
      pool.shutdown
      unless pool.wait_for_termination(10)
        pool.kill
        raise 'fork/join pool did not terminate'
      end
    end

    def wait_until(timeout = 10)
      deadline = Concurrent.monotonic_time + timeout
      until yield
        return false if Concurrent.monotonic_time > deadline
        sleep 0.01
      end
      true
    end

    context 'minimal example: recursive divide and conquer' do

      let(:pool_size) { 1 }

      it 'computes on a single worker without deadlocking' do
        data = (1..128).to_a
        executions = Concurrent::AtomicFixnum.new(0)
        sum = nil
        sum = lambda do |lo, hi|
          executions.increment
          if hi - lo <= 8
            data[lo...hi].reduce(0, :+)
          else
            mid = (lo + hi) / 2
            left = pool.fork(lo, mid, &sum)
            right = pool.fork(mid, hi, &sum)
            left.join + right.join
          end
        end

        root = pool.fork(0, data.size, &sum)
        expect(root.join(10)).to eq data.sum
        # 16 leaf segments + 15 internal nodes, each executed exactly once
        expect(executions.value).to eq 31
      end
    end

    context 'when all workers are blocked in join' do

      let(:pool_size) { 2 }

      it 'lets workers help execute their forked tasks' do
        parents_started = Concurrent::CountDownLatch.new(2)
        children_started = Concurrent::CountDownLatch.new(2)
        gate = Concurrent::Event.new
        parent_executions = Concurrent::AtomicFixnum.new(0)
        child_executions = Concurrent::AtomicFixnum.new(0)

        parent = lambda do |id|
          parent_executions.increment
          parents_started.count_down
          parents_started.wait # pin: both workers occupied by parents
          child = pool.fork(id) do |child_id|
            child_executions.increment
            children_started.count_down
            gate.wait # hold each child inside its parent's join
            child_id * 10
          end
          child.join + id
        end

        handles = [pool.fork(1, &parent), pool.fork(2, &parent)]

        # the pinned moment: every worker is inside join, children pending
        expect(children_started.wait(10)).to be true
        expect(pool.joining_worker_count).to eq 2

        gate.set
        expect(handles.map { |handle| handle.join(10) }).to match_array [11, 22]
        expect(parent_executions.value).to eq 2
        expect(child_executions.value).to eq 2
      end

      it 'never grows beyond its fixed worker count' do
        expect(pool.size).to eq 2
        expect(pool.joining_worker_count).to eq 0
      end
    end

    context 'nested forking and joining multiple results' do

      let(:pool_size) { 4 }

      it 'combines results from a tree of tasks' do
        executions = Concurrent::AtomicFixnum.new(0)
        tree = nil
        tree = lambda do |depth, value|
          executions.increment
          if depth.zero?
            value
          else
            children = (1..3).map { |i| pool.fork(depth - 1, value * 10 + i, &tree) }
            children.map(&:join).reduce(0, :+)
          end
        end

        expected = nil
        expected = lambda do |depth, value|
          if depth.zero?
            value
          else
            (1..3).map { |i| expected.call(depth - 1, value * 10 + i) }.reduce(0, :+)
          end
        end

        root = pool.fork(3, 0, &tree)
        expect(root.join(10)).to eq expected.call(3, 0)
        expect(executions.value).to eq 1 + 3 + 9 + 27
      end
    end

    context 'exception propagation' do

      let(:pool_size) { 2 }

      it 're-raises task exceptions in external joiners' do
        boom = Class.new(StandardError)
        handle = pool.fork { raise boom, 'boom' }
        expect { handle.join(10) }.to raise_error(boom, 'boom')
      end

      it 'propagates exceptions through nested joins' do
        boom = Class.new(StandardError)
        leaf = lambda { raise boom, 'deep boom' }
        parent = lambda { pool.fork(&leaf).join }
        grandparent = lambda { pool.fork(&parent).join }

        handle = pool.fork(&grandparent)
        expect { handle.join(10) }.to raise_error(boom, 'deep boom')
      end
    end

    context 'dependency cycle detection' do

      let(:pool_size) { 2 }

      it 'raises DependencyCycleError for mutually waiting tasks' do
        started = Concurrent::CountDownLatch.new(2)
        handle_a = nil
        handle_b = nil
        handle_a = pool.fork do
          started.count_down
          started.wait
          handle_b.join
        end
        handle_b = pool.fork do
          started.count_down
          started.wait
          handle_a.join
        end

        expect { handle_a.join(10) }.to raise_error(described_class::DependencyCycleError)
        expect { handle_b.join(10) }.to raise_error(described_class::DependencyCycleError)
      end

      it 'detects a task joining itself' do
        assigned = Concurrent::CountDownLatch.new(1)
        handle = nil
        handle = pool.fork do
          assigned.wait
          handle.join
        end
        assigned.count_down
        expect { handle.join(10) }.to raise_error(described_class::DependencyCycleError)
      end
    end

    context 'shutdown' do

      let(:pool_size) { 2 }

      it 'gives joins on never-started tasks a definite outcome' do
        gate = Concurrent::Event.new
        blocker_started = Concurrent::CountDownLatch.new(2)
        2.times do
          pool.fork do
            blocker_started.count_down
            gate.wait
          end
        end
        expect(blocker_started.wait(10)).to be true

        pending = pool.fork { :never }
        pool.shutdown

        expect { pending.join(10) }.to raise_error(described_class::ShutdownError)
        expect { pool.fork { :rejected } }.to raise_error(Concurrent::RejectedExecutionError)
        expect(pool.post { :rejected }).to eq false

        gate.set
        expect(pool.wait_for_termination(10)).to be true
      end

      it 'gives joins blocked on running tasks a definite outcome' do
        gate = Concurrent::Event.new
        blocker_started = Concurrent::CountDownLatch.new(1)
        blocker = pool.fork do
          blocker_started.count_down
          gate.wait
          :done
        end
        expect(blocker_started.wait(10)).to be true

        parent = pool.fork { blocker.join }
        expect(wait_until { pool.joining_worker_count == 1 }).to be true

        pool.shutdown

        expect { parent.join(10) }.to raise_error(described_class::ShutdownError)
        expect { blocker.join(10) }.to raise_error(described_class::ShutdownError)

        gate.set
        expect(pool.wait_for_termination(10)).to be true
      end
    end

    context 'exactly-once execution' do

      let(:pool_size) { 4 }

      it 'runs every externally submitted task exactly once' do
        counts = Array.new(100) { Concurrent::AtomicFixnum.new(0) }
        handles = (0...100).map do |i|
          pool.fork(i) { |value| counts[value].increment; value * 2 }
        end

        expect(handles.map { |handle| handle.join(10) }).to eq((0...100).map { |i| i * 2 })
        expect(counts.map(&:value)).to all(eq 1)
        expect(pool.completed_task_count).to eq 100
        expect(pool.scheduled_task_count).to eq 0
      end

      it 'runs every helped task exactly once' do
        executions = Concurrent::AtomicFixnum.new(0)
        parent = lambda do |id|
          children = (1..3).map do |i|
            pool.fork(id * 10 + i) { |value| executions.increment; value }
          end
          executions.increment
          children.map(&:join).reduce(0, :+)
        end

        handles = (0...20).map { |id| pool.fork(id, &parent) }
        results = handles.map { |handle| handle.join(10) }

        expect(results).to eq((0...20).map { |id| (1..3).map { |i| id * 10 + i }.reduce(0, :+) })
        expect(executions.value).to eq 20 + 60
      end
    end
  end
end
