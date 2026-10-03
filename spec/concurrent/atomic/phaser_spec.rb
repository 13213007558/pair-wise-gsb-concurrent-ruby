require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/phaser'

module Concurrent

  RSpec.describe Phaser do

    describe '#initialize' do
      it 'accepts zero parties' do
        phaser = described_class.new(0)

        expect(phaser.registered_parties).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser).not_to be_terminated
      end

      it 'does not terminate until the first registered party exits' do
        phaser = described_class.new(0)

        expect(phaser.register).to eq 0
        expect(phaser.arrive_and_deregister).to eq 0

        expect(phaser).to be_terminated
      end

      it 'rejects an invalid party count' do
        expect { described_class.new(-1) }.to raise_error(ArgumentError)
        expect { described_class.new('x') }.to raise_error(ArgumentError)
      end

      it 'rejects an invalid parent' do
        expect { described_class.new(:parent, 1) }.to raise_error(ArgumentError)
      end

      it 'registers a child as one parent party' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 3)

        expect(child.parent).to eq parent
        expect(child.root).to eq parent
        expect(parent.registered_parties).to eq 2
        expect(child.registered_parties).to eq 3
      end
    end

    describe '#register' do
      it 'adds parties to the current phase' do
        phaser = described_class.new(1)

        expect(phaser.register(2)).to eq 0

        expect(phaser.registered_parties).to eq 3
        expect(phaser.unarrived_parties).to eq 3
      end

      it 'puts registration at a completed local phase into the next phase' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 1)
        ready   = CountDownLatch.new(1)
        release = CountDownLatch.new(1)
        child_worker = in_thread do
          child.arrive
          ready.count_down
          release.wait
          child.arrive_and_await_advance
        end

        ready.wait(1)
        repeat_until_success { expect(child.unarrived_parties).to eq 0 }
        expect(parent.unarrived_parties).to eq 1

        registering = in_thread do
          registered_phase = child.register
          advanced_phase   = child.arrive_and_await_advance
          [registered_phase, advanced_phase]
        end
        repeat_until_success do
          expect(child.instance_variable_get(:@register_waiting_count)).to eq 1
        end

        expect(child.phase).to eq 0
        expect(parent.phase).to eq 0

        parent_worker = in_thread do
          first_advance  = parent.arrive_and_await_advance
          second_advance = parent.arrive_and_await_advance
          [first_advance, second_advance]
        end
        repeat_until_success { expect(parent.phase).to eq 1 }
        repeat_until_success { expect(child.registered_parties).to eq 2 }
        expect(child.phase).to eq 1

        release.count_down
        expect(registering.value).to eq [1, 2]
        expect(parent_worker.value).to eq [1, 2]
        join_with child_worker
      end

      it 'keeps a post-boundary zero-party registration in the next phase when the action continues' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 1) { false }

        expect(child.arrive_and_deregister).to eq 0
        expect(child.phase).to eq 0

        registering = in_thread { child.register }
        repeat_until_success { expect(child.instance_variable_get(:@register_waiting_count)).to eq 1 }

        expect(parent.arrive_and_await_advance).to eq 1
        expect(registering.value).to eq 1
        expect(child.registered_parties).to eq 1
        expect(child.unarrived_parties).to eq 1
      end

      it 'cannot register after termination' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister

        expect(phaser).to be_terminated
        expect { phaser.register }.to raise_error(IllegalOperationError)
      end
    end

    describe '#arrive' do
      it 'advances and resets party counts when every party arrives' do
        phaser = described_class.new(2)
        latch    = CountDownLatch.new(1)

        waiter = in_thread do
          expect(phaser.arrive).to eq 0
          phaser.await_advance(0)
          latch.count_down
        end

        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }
        expect(phaser.phase).to eq 0
        expect(phaser.arrive).to eq 0

        expect(latch.wait(1)).to be_truthy
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 2
        expect(phaser.unarrived_parties).to eq 2
        join_with waiter
      end

      it 'rejects another arrival once the current generation is fully arrived' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 2)

        expect(child.arrive).to eq 0
        expect(child.arrive).to eq 0
        expect(child.phase).to eq 0
        expect { child.arrive }.to raise_error(IllegalOperationError)

        expect(parent.arrive_and_await_advance).to eq 1
        expect(child.phase).to eq 1
      end

      it 'does not advance the parent until every child party has arrived' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 2)
        start   = CountDownLatch.new(1)
        release = CountDownLatch.new(1)

        first = in_thread do
          child.arrive
          start.count_down
          release.wait
          child.await_advance(0)
        end
        second = in_thread do
          start.wait(1)
          release.wait
          child.arrive
          child.await_advance(0)
        end
        parent_worker = in_thread do
          start.wait(1)
          parent.arrive_and_await_advance
        end

        start.wait(1)
        repeat_until_success { expect(child.arrived_parties).to eq 1 }
        repeat_until_success { expect(parent.arrived_parties).to eq 1 }
        expect(parent.unarrived_parties).to eq 1
        expect(parent.phase).to eq 0
        expect(child.phase).to eq 0

        release.count_down
        expect(parent_worker.value).to eq 1
        expect(first.value).to eq 1
        expect(second.value).to eq 1
        expect(child.phase).to eq 1
      end

      it 'allows the number of child parties to change without new worker threads' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 2)
        queues = Array.new(3) { Queue.new }

        workers = Array.new(3).map.with_index do |_, worker_index|
          in_thread do
            loop do
              command = queues[worker_index].pop
              break if command == :stop

              case command
              when :advance
                child.arrive_and_await_advance
              when :deregister
                child.arrive_and_deregister
              end
            end
          end
        end

        queues[0] << :advance
        queues[1] << :advance
        expect(parent.arrive_and_await_advance).to eq 1
        expect(child.register).to eq 1

        queues[0] << :deregister
        queues[1] << :deregister
        queues[2] << :advance
        expect(parent.arrive_and_await_advance).to eq 2
        expect(child.registered_parties).to eq 1

        queues[2] << :deregister
        repeat_until_success { expect(child).to be_terminated }
        expect(parent.arrive_and_deregister).to eq 2

        expect(parent.phase).to eq(-3)
        expect(child.phase).to eq(-3)
        expect(parent).to be_terminated
        expect(child).to be_terminated

        queues.each { |queue| queue << :stop }
        join_with(workers)
      end
    end

    describe '#arrive_and_deregister' do
      it 'terminates after the last registered party exits' do
        phaser = described_class.new(1)

        expect(phaser.arrive_and_deregister).to eq 0

        expect(phaser.registered_parties).to eq 0
        expect(phaser.unarrived_parties).to eq 0
        expect(phaser.phase).to eq(-1)
        expect(phaser).to be_terminated
      end

      it 'reduces the number of parties for the next phase' do
        phaser = described_class.new(2)
        latch    = CountDownLatch.new(1)

        worker = in_thread do
          phaser.arrive_and_await_advance
          latch.count_down
        end

        expect(phaser.arrive_and_deregister).to eq 0
        expect(latch.wait(1)).to be_truthy
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
        join_with worker
      end
    end

    describe '#await_advance' do
      it 'returns immediately for a stale phase' do
        phaser = described_class.new(1)

        expect(phaser.arrive_and_await_advance).to eq 1
        expect(phaser.await_advance(0)).to eq 1
      end

      it 'rejects waiting for a future phase' do
        phaser = described_class.new(1)

        expect { phaser.await_advance(1) }.to raise_error(ArgumentError)
      end

      it 'does not count waiting as an arrival or deregistration' do
        phaser = described_class.new(1)
        waiter  = in_thread { phaser.await_advance(0) }

        repeat_until_success { expect(phaser.number_waiting).to eq 1 }
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1

        expect(phaser.arrive_and_await_advance).to eq 1
        expect(waiter.value).to eq 1
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
        expect(phaser.number_waiting).to eq 0
      end

      it 'returns false on timeout without changing the phase or parties' do
        phaser = described_class.new(1)

        expect(phaser.await_advance(0, 0.001)).to eq false

        expect(phaser.phase).to eq 0
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
        expect(phaser.number_waiting).to eq 0
      end

      it 'returns the negative phase after termination instead of timing out' do
        phaser = described_class.new(1)
        waiter  = in_thread { phaser.await_advance(0, 5) }

        repeat_until_success { expect(phaser.number_waiting).to eq 1 }
        expect(phaser.force_terminate).to eq(-1)
        expect(waiter.value).to eq(-1)
      end

      it 'leaves party counts intact when the waiting thread raises' do
        phaser = described_class.new(2)
        waiter = Thread.new { phaser.await_advance(0) }
        waiter.report_on_exception = false

        repeat_until_success { expect(phaser.number_waiting).to eq 1 }
        waiter.raise(RuntimeError, 'boom')

        expect { waiter.join }.to raise_error(RuntimeError, 'boom')
        expect(phaser.phase).to eq 0
        expect(phaser.registered_parties).to eq 2
        expect(phaser.unarrived_parties).to eq 2
        expect(phaser.number_waiting).to eq 0
      end
    end

    describe '#force_terminate' do
      it 'terminates descendants and wakes waiters' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 1)
        waiter = in_thread { child.await_advance(0, 5) }

        repeat_until_success { expect(child.number_waiting).to eq 1 }
        expect(parent.force_terminate).to eq(-1)

        expect(waiter.value).to eq(-1)
        expect(parent).to be_terminated
        expect(child).to be_terminated
        expect { parent.arrive }.to raise_error(IllegalOperationError)
      end

      it 'does not let a terminated child silently remove its parent party' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 1)

        expect(child.force_terminate).to eq(-1)
        expect(parent.phase).to eq 0
        expect(parent.registered_parties).to eq 2
        expect(parent.unarrived_parties).to eq 2

        expect(parent.arrive).to eq 0
        expect(parent.phase).to eq 0
        expect(parent.arrive_and_deregister).to eq 0
        expect(parent.phase).to eq 1
        expect(parent.registered_parties).to eq 1
        expect(parent.unarrived_parties).to eq 1
        expect(parent.force_terminate).to eq(-2)
      end
    end

    describe 'phase action' do
      it 'terminates the tree when the action raises' do
        phaser = described_class.new(1) { raise 'boom' }

        expect { phaser.arrive_and_await_advance }.to raise_error(RuntimeError, 'boom')
        expect(phaser).to be_terminated
        expect(phaser.phase).to eq(-1)
      end

      it 'treats a child action failure as its final arrival and deregistration' do
        parent = described_class.new(1)
        child  = described_class.new(parent, 1) { raise 'boom' }

        expect { child.arrive }.to raise_error(RuntimeError, 'boom')
        expect(child).to be_terminated
        expect(parent.phase).to eq 0
        expect(parent.registered_parties).to eq 1
        expect(parent.unarrived_parties).to eq 1

        expect(parent.arrive_and_deregister).to eq 0
        expect(parent).to be_terminated
      end
    end
  end
end
