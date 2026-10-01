require 'concurrent/atomic/phaser'

module Concurrent

  RSpec.describe Phaser do

    context '#initialize' do

      it 'defaults to zero parties at phase zero' do
        phaser = described_class.new
        expect(phaser.registered_parties).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.terminated?).to be false
      end

      it 'accepts an initial number of parties' do
        expect(described_class.new(3).registered_parties).to eq 3
      end

      it 'raises an exception if parties is negative' do
        expect { described_class.new(-1) }.to raise_error(ArgumentError)
      end

      it 'raises an exception if parties is not an integer' do
        expect { described_class.new('foo') }.to raise_error(ArgumentError)
      end
    end

    describe '#register and #bulk_register' do

      it 'increases the number of registered parties' do
        phaser = described_class.new(1)
        expect(phaser.register).to eq 0
        expect(phaser.registered_parties).to eq 2
        expect(phaser.bulk_register(3)).to eq 0
        expect(phaser.registered_parties).to eq 5
        expect(phaser.unarrived_parties).to eq 5
      end

      it 'rejects invalid bulk counts' do
        phaser = described_class.new
        expect { phaser.bulk_register(-1) }.to raise_error(ArgumentError)
        expect { phaser.bulk_register(1.5) }.to raise_error(ArgumentError)
      end
    end

    describe '#arrive' do

      it 'counts the arrival without blocking or advancing early' do
        phaser = described_class.new(2)
        expect(phaser.arrive).to eq 0
        expect(phaser.arrived_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
        expect(phaser.phase).to eq 0
      end

      it 'advances the phase on the last arrival' do
        phaser = described_class.new(2)
        phaser.arrive
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.arrived_parties).to eq 0
      end
    end

    describe '#arrive_and_await_advance' do

      it 'blocks until all parties arrive, then releases everyone' do
        phaser   = described_class.new(3)
        released = Queue.new

        t1 = in_thread { released << phaser.arrive_and_await_advance }
        t2 = in_thread { released << phaser.arrive_and_await_advance }

        repeat_until_success { expect(phaser.arrived_parties).to eq 2 }
        is_sleeping t1
        is_sleeping t2

        # two of three arrivals must not release anybody
        expect(phaser.phase).to eq 0
        expect(released.empty?).to be true

        expect(phaser.arrive_and_await_advance).to eq 0
        expect(phaser.phase).to eq 1

        expect(released.pop).to eq 0
        expect(released.pop).to eq 0
        join_with [t1, t2]
      end

      it 'counts each arrival toward exactly one phase' do
        phaser = described_class.new(2)

        t = in_thread do
          phaser.arrive_and_await_advance
          phaser.arrive_and_await_advance
        end

        expect(phaser.arrive_and_await_advance).to eq 0
        expect(phaser.phase).to eq 1

        # the thread's second arrival alone must not complete phase 1:
        # the arrivals of phase 0 were reset when the phase advanced
        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }
        expect(phaser.phase).to eq 1

        expect(phaser.arrive_and_await_advance).to eq 1
        expect(phaser.phase).to eq 2
        join_with t
      end
    end

    describe '#arrive_and_deregister' do

      it 'reduces the registered parties and drops out of future phases' do
        phaser = described_class.new(2)
        expect(phaser.arrive_and_deregister).to eq 0
        expect(phaser.registered_parties).to eq 1
        expect(phaser.phase).to eq 0

        # the remaining party now advances phases on its own
        expect(phaser.arrive_and_await_advance).to eq 0
        expect(phaser.phase).to eq 1
      end

      it 'advances the phase when the last unarrived party deregisters' do
        phaser   = described_class.new(2)
        released = Queue.new

        t = in_thread { released << phaser.arrive_and_await_advance }
        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }
        is_sleeping t

        # the second party drops out instead of arriving: the phase completes
        expect(phaser.arrive_and_deregister).to eq 0
        expect(phaser.registered_parties).to eq 1
        expect(phaser.phase).to eq 1
        expect(released.pop).to eq 0
        join_with t
      end
    end

    describe '#await_advance' do

      it 'returns immediately for a stale phase' do
        phaser = described_class.new(1)
        phaser.arrive
        expect(phaser.await_advance(0)).to eq 1
      end

      it 'blocks until the phase advances' do
        phaser   = described_class.new(2)
        observed = Queue.new

        t = in_thread { observed << phaser.await_advance(0) }
        is_sleeping t
        expect(observed.empty?).to be true

        phaser.arrive
        expect(phaser.arrive_and_await_advance).to eq 0
        expect(observed.pop).to eq 1
        join_with t
      end
    end

    describe '#await_advance_interruptibly' do

      it 'ends only the current wait on timeout without disturbing other parties' do
        phaser   = described_class.new(2)
        released = Queue.new

        t = in_thread { released << phaser.arrive_and_await_advance }
        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }

        # the wait times out, but counts and phase are left untouched
        expect(phaser.await_advance_interruptibly(0, 0.1)).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.arrived_parties).to eq 1
        expect(phaser.registered_parties).to eq 2
        is_sleeping t

        # the phase can still be completed normally afterwards
        expect(phaser.arrive_and_await_advance).to eq 0
        expect(phaser.phase).to eq 1
        expect(released.pop).to eq 0
        join_with t
      end

      it 'returns the new phase when the phase advances in time' do
        phaser = described_class.new(1)
        t = in_thread { phaser.arrive_and_await_advance }
        repeat_until_success { expect(phaser.phase).to eq 1 }
        expect(phaser.await_advance_interruptibly(0, 5)).to eq 1
        join_with t
      end
    end

    context 'with zero registered parties' do

      it 'raises on arrival methods' do
        phaser = described_class.new
        expect { phaser.arrive }.to raise_error(IllegalOperationError)
        expect { phaser.arrive_and_await_advance }.to raise_error(IllegalOperationError)
        expect { phaser.arrive_and_deregister }.to raise_error(IllegalOperationError)
      end

      it 'returns immediately from await_advance for a stale phase' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister # zero parties left, phase advances
        expect(phaser.registered_parties).to eq 0
        expect(phaser.await_advance(0)).to eq 1
        expect(phaser.await_advance_interruptibly(0, 0.01)).to eq 1
      end
    end

    describe '#force_termination' do

      it 'releases waiting parties with a negative phase' do
        phaser   = described_class.new(3)
        released = Queue.new

        t1 = in_thread { released << phaser.arrive_and_await_advance }
        t2 = in_thread { released << phaser.await_advance(0) }
        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }
        is_sleeping t1
        is_sleeping t2

        phaser.force_termination

        expect(phaser.terminated?).to be true
        2.times { expect(released.pop).to be < 0 }
        join_with [t1, t2]
      end

      it 'makes synchronization methods return negative values without blocking' do
        phaser = described_class.new(2)
        phaser.force_termination

        expect(phaser.phase).to be < 0
        expect(phaser.register).to be < 0
        expect(phaser.bulk_register(2)).to be < 0
        expect(phaser.arrive).to be < 0
        expect(phaser.arrive_and_await_advance).to be < 0
        expect(phaser.arrive_and_deregister).to be < 0
        expect(phaser.await_advance(0)).to be < 0
        expect(phaser.await_advance_interruptibly(0, 5)).to be < 0

        # registration and arrival counts are left untouched
        expect(phaser.registered_parties).to eq 2
        expect(phaser.arrived_parties).to eq 0
      end
    end

    describe 'registration racing a phase change' do

      it 'joins the in-progress phase when registering before it completes' do
        phaser          = described_class.new(2) # main thread + worker
        worker_released = Queue.new

        worker = in_thread { worker_released << phaser.arrive_and_await_advance }
        repeat_until_success { expect(phaser.arrived_parties).to eq 1 }
        is_sleeping worker

        # register while phase 0 is in progress: the new party joins phase 0
        expect(phaser.register).to eq 0
        expect(phaser.registered_parties).to eq 3

        # main arrives: two of three arrived, nobody may be released yet
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 0
        expect(worker_released.empty?).to be true
        is_sleeping worker

        # the late party arrives: phase 0 completes exactly once
        late = in_thread { phaser.arrive_and_await_advance }
        expect(worker_released.pop).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.arrived_parties).to eq 0
        join_with [worker, late]
      end

      it 'assigns a registration racing the advancing arrival to exactly one phase' do
        100.times do
          phaser        = described_class.new(1)
          registered_in = Queue.new

          t = in_thread { registered_in << phaser.register }
          phaser.arrive # completes phase 0 and advances to phase 1
          join_with t

          # the registration was ordered either before or after the advance,
          # never lost and never counted in both phases
          expect([0, 1]).to include registered_in.pop
          expect(phaser.registered_parties).to eq 2
        end
      end
    end

    it 'coordinates a pipeline whose party count changes every round' do
      phaser         = described_class.new(1) # the main thread participates
      rounds         = [2, 3, 1]
      arrived_rounds = Queue.new

      rounds.each_with_index do |n, round|
        threads = n.times.map do
          phaser.register
          in_thread(round) do |r|
            phaser.arrive_and_await_advance # wait for the round to start
            arrived_rounds << r
            phaser.arrive_and_deregister    # drop out of the next rounds
          end
        end

        phaser.arrive_and_await_advance # release the round once all arrived
        join_with threads

        n.times { expect(arrived_rounds.pop).to eq round }
        expect(phaser.phase).to eq round + 1
        expect(phaser.registered_parties).to eq 1
      end
    end
  end
end
