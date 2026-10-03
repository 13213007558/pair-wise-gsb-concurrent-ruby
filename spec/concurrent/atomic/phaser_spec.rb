require 'concurrent/atomic/phaser'
require 'concurrent/atomic/count_down_latch'

module Concurrent

  RSpec.describe Phaser do

    # A phaser which stays usable after all of its parties deregistered,
    # so it can be re-populated for the next phase.
    class ReusableTestPhaser < Phaser
      protected

      def on_advance(phase, registered_parties)
        false
      end
    end

    context '#initialize' do

      it 'starts at phase 0 with the given number of parties' do
        phaser = described_class.new(3)
        expect(phaser.phase).to eq 0
        expect(phaser.registered_parties).to eq 3
        expect(phaser.arrived_parties).to eq 0
        expect(phaser.unarrived_parties).to eq 3
        expect(phaser).not_to be_terminated
      end

      it 'allows zero parties' do
        expect(described_class.new.registered_parties).to eq 0
      end

      it 'raises an exception if parties is negative' do
        expect { described_class.new(-1) }.to raise_error(ArgumentError)
      end

      it 'raises an exception if parties is not an integer' do
        expect { described_class.new('foo') }.to raise_error(ArgumentError)
      end

      it 'raises an exception if parent is not a phaser' do
        expect { described_class.new(1, :nope) }.to raise_error(ArgumentError)
      end
    end

    context '#register' do

      it 'adds parties and returns the phase they joined' do
        phaser = described_class.new(1)
        expect(phaser.register).to eq 0
        expect(phaser.bulk_register(2)).to eq 0
        expect(phaser.registered_parties).to eq 4
      end

      it 'rejects a non-positive bulk count' do
        phaser = described_class.new
        expect { phaser.bulk_register(0) }.to raise_error(ArgumentError)
      end

      it 'is counted in the new generation when it races with an advance' do
        phaser    = described_class.new(1)
        advancing = CountDownLatch.new(1)
        proceed   = CountDownLatch.new(1)

        # intercept the advance of phase 0 while the phaser lock is held
        phaser.define_singleton_method(:on_advance) do |_phase, _parties|
          advancing.count_down
          proceed.wait
          false
        end

        arriver = in_thread { phaser.arrive }
        advancing.wait # the advance of phase 0 is now in flight

        registered_phase = Queue.new
        registrar        = in_thread { registered_phase << phaser.register }
        # the registration cannot sneak into the phase being advanced,
        # it blocks until the advance completes
        repeat_until_success { expect(registrar.status).to eq 'sleep' }

        proceed.count_down
        expect(registered_phase.pop).to eq 1
        join_with [arriver, registrar]
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 2
      end
    end

    context '#arrive' do

      it 'advances the phase once all parties arrived' do
        phaser = described_class.new(2)
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.arrived_parties).to eq 1
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.arrived_parties).to eq 0
      end

      it 'raises when more threads arrive than there are parties' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister # no parties left
        expect { phaser.arrive }.to raise_error(IllegalOperationError)
      end

      it 'raises when arriving on a phaser without parties' do
        expect { described_class.new.arrive }.to raise_error(IllegalOperationError)
      end
    end

    context '#arrive_and_deregister' do

      it 'reduces the parties and does not complete the phase for the others' do
        phaser = described_class.new(2)
        phaser.arrive_and_deregister
        expect(phaser.registered_parties).to eq 1
        # the remaining party has not arrived yet, the phase must not advance
        expect(phaser.phase).to eq 0
        phaser.arrive
        expect(phaser.phase).to eq 1
      end

      it 'advances when the only unarrived party deregisters' do
        phaser = described_class.new(2)
        phaser.arrive
        phaser.arrive_and_deregister
        expect(phaser.phase).to eq 1
      end

      it 'raises when there is no party to deregister' do
        phaser = described_class.new
        expect { phaser.arrive_and_deregister }.to raise_error(IllegalOperationError)
      end

      it 'terminates when the last party deregisters' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister
        expect(phaser).to be_terminated
        expect(phaser.registered_parties).to eq 0
      end
    end

    context '#await_advance' do

      it 'blocks until the given phase completes and returns the new phase' do
        phaser = described_class.new(2)
        in_thread { phaser.arrive }

        expect(phaser.arrive_and_await_advance).to eq 1
      end

      it 'returns immediately when waiting on a stale phase' do
        phaser = described_class.new(1)
        phaser.arrive # completes phase 0
        expect(phaser.await_advance(0)).to eq 1
      end

      it 'returns immediately when waiting on a phase not reached yet' do
        phaser = described_class.new(1)
        expect(phaser.await_advance(3)).to eq 0
      end

      it 'returns nil on timeout without shrinking the barrier' do
        phaser = described_class.new(2)
        phaser.arrive

        expect(phaser.await_advance(0, 0.1)).to be_nil
        expect(phaser.registered_parties).to eq 2
        expect(phaser.arrived_parties).to eq 1
        expect(phaser.phase).to eq 0

        phaser.arrive # the barrier still completes normally
        expect(phaser.phase).to eq 1
      end

      it 'keeps the arrival when arrive_and_await_advance times out' do
        phaser = described_class.new(2)
        expect(phaser.arrive_and_await_advance(0.1)).to be_nil
        expect(phaser.registered_parties).to eq 2
        expect(phaser.arrived_parties).to eq 1
        phaser.arrive
        expect(phaser.phase).to eq 1
      end

      it 'is not corrupted when a waiting thread is killed' do
        phaser = described_class.new(2)
        phaser.arrive

        thread = Thread.new { phaser.await_advance(0) }
        repeat_until_success { expect(thread.status).to eq 'sleep' }
        thread.kill
        thread.join

        # the dead waiter must not have shrunk the barrier
        expect(phaser.registered_parties).to eq 2
        expect(phaser.arrived_parties).to eq 1

        phaser.arrive
        expect(phaser.phase).to eq 1
      end
    end

    context '#force_termination' do

      it 'releases waiters and rejects further registration and arrival' do
        phaser = described_class.new(2)
        phaser.arrive
        entered = CountDownLatch.new(1)
        result  = Queue.new

        waiter = in_thread do
          entered.count_down
          result << phaser.await_advance(0)
        end
        entered.wait
        repeat_until_success { expect(waiter.status).to eq 'sleep' }

        phaser.force_termination
        expect(result.pop).to eq 0
        join_with waiter

        expect(phaser).to be_terminated
        expect { phaser.register }.to raise_error(IllegalOperationError)
        expect { phaser.arrive }.to raise_error(IllegalOperationError)
        expect { phaser.arrive_and_deregister }.to raise_error(IllegalOperationError)
        # waiting on a terminated phaser never blocks
        expect(phaser.await_advance(0)).to eq 0
      end
    end

    context 'tiering' do

      it 'occupies exactly one party on the parent while it has parties' do
        parent = described_class.new(1)
        child  = described_class.new(2, parent)
        expect(child.parent).to eq parent
        expect(parent.registered_parties).to eq 2

        child.arrive_and_deregister
        expect(parent.registered_parties).to eq 2 # still has a party left

        child.arrive_and_deregister # last party leaves, slot is released
        expect(child).to be_terminated
        expect(parent.registered_parties).to eq 1
        expect(parent.phase).to eq 0 # the main thread has not arrived yet

        parent.arrive
        expect(parent.phase).to eq 1
      end

      it 're-registers with the parent when growing from zero parties' do
        parent = described_class.new(1)
        child  = ReusableTestPhaser.new(0, parent)
        expect(parent.registered_parties).to eq 1

        child.register
        expect(parent.registered_parties).to eq 2

        child.arrive_and_deregister
        expect(parent.registered_parties).to eq 1
      end

      it 'does not advance the parent before every party of the child group arrived' do
        parent = described_class.new(1) # the main thread
        child  = described_class.new(2, parent)

        parent.arrive # main is done for this phase, the group is outstanding

        first_gate  = CountDownLatch.new(1)
        second_gate = CountDownLatch.new(1)
        first_done  = CountDownLatch.new(1)
        second_done = CountDownLatch.new(1)

        in_thread { first_gate.wait; child.arrive_and_deregister; first_done.count_down }
        in_thread { second_gate.wait; child.arrive_and_deregister; second_done.count_down }

        first_gate.count_down
        first_done.wait
        # one subtask of the group is still outstanding: nothing may advance
        expect(child.phase).to eq 0
        expect(child.registered_parties).to eq 1
        expect(parent.phase).to eq 0
        expect(parent.arrived_parties).to eq 1

        second_gate.count_down
        second_done.wait
        # the group drained, releasing its single slot on the parent
        expect(child).to be_terminated
        expect(parent.phase).to eq 1
        expect(parent.registered_parties).to eq 1
      end

      it 'supports a subgroup whose size changes every phase' do
        parent = described_class.new(1) # the main thread
        group  = ReusableTestPhaser.new(0, parent)
        sizes  = [2, 3, 1]

        sizes.each_with_index do |size, phase|
          size.times { group.register }
          expect(parent.registered_parties).to eq 2

          gate    = CountDownLatch.new(1)
          threads = Array.new(size) do
            in_thread { gate.wait; group.arrive_and_deregister }
          end
          gate.count_down

          # the main thread is released only once the whole group drained
          expect(parent.arrive_and_await_advance).to eq phase + 1
          join_with threads
          expect(group.registered_parties).to eq 0
          expect(parent.registered_parties).to eq 1
        end

        expect(parent.phase).to eq sizes.size
        expect(group.phase).to eq sizes.size
        expect(group).not_to be_terminated
      end
    end

    context 'multiple phases' do

      it 'advances through many phases with a fixed set of threads' do
        phaser = described_class.new(3)
        phases = 5

        threads = Array.new(2) do
          in_thread { phases.times { phaser.arrive_and_await_advance } }
        end
        phases.times { phaser.arrive_and_await_advance } # main participates too
        join_with threads

        expect(phaser.phase).to eq phases
        expect(phaser.registered_parties).to eq 3
        expect(phaser.arrived_parties).to eq 0
      end
    end
  end
end
