require 'concurrent/atomic/phaser'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/atomic_fixnum'

module Concurrent

  RSpec.describe Phaser do

    describe '#initialize' do

      it 'accepts zero parties' do
        phaser = described_class.new(0)
        expect(phaser.phase).to eq 0
        expect(phaser.registered_parties).to eq 0
        expect(phaser.unarrived_parties).to eq 0
        expect(phaser).not_to be_terminated
      end

      it 'registers the given number of parties' do
        phaser = described_class.new(3)
        expect(phaser.registered_parties).to eq 3
        expect(phaser.unarrived_parties).to eq 3
      end

      it 'defaults to zero parties' do
        phaser = described_class.new
        expect(phaser.registered_parties).to eq 0
      end

      it 'raises an exception if the number of parties is not an integer' do
        expect { described_class.new('foo') }.to raise_error(ArgumentError)
      end

      it 'raises an exception if the number of parties is negative' do
        expect { described_class.new(-1) }.to raise_error(ArgumentError)
      end
    end

    describe '#register' do

      it 'adds a party which has to arrive in the current phase' do
        phaser = described_class.new(1)
        expect(phaser.register).to eq 0
        expect(phaser.registered_parties).to eq 2
        expect(phaser.unarrived_parties).to eq 2

        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.unarrived_parties).to eq 2
      end

      it 'can register several parties at once' do
        phaser = described_class.new(0)
        expect(phaser.register(3)).to eq 0
        expect(phaser.registered_parties).to eq 3
        expect(phaser.unarrived_parties).to eq 3
      end

      it 'raises on a non positive number of parties' do
        phaser = described_class.new(0)
        expect { phaser.register(0) }.to raise_error(ArgumentError)
        expect { phaser.register(-2) }.to raise_error(ArgumentError)
        expect { phaser.register('x') }.to raise_error(ArgumentError)
      end

      it 'advances a drained phaser so the registration belongs to one phase' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister
        # all parties left, the phaser terminated by default
        expect(phaser.phase).to eq(-1)
        expect(phaser).to be_terminated
      end

      it 'assigns a registration on a drained but non terminating phaser to the new phase' do
        phaser = described_class.new(1) { false }
        phaser.arrive_and_deregister
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 0

        expect(phaser.register).to eq 2
        expect(phaser.phase).to eq 2
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
      end

      it 'keeps a pristine zero party phaser at phase zero' do
        phaser = described_class.new(0) { false }
        expect(phaser.register).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.registered_parties).to eq 1
      end

      it 'has no effect on a terminated phaser' do
        phaser = described_class.new(1)
        phaser.force_termination
        expect(phaser.register).to eq(-1)
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
      end

      it 'is serialized with phase completion so one arrival is never counted twice' do
        # One party has already arrived, so the contending arrival is the last
        # one and will complete the phase. The main thread holds the phaser
        # lock until both the arriving and the registering thread are queued
        # behind it, then lets them run. Whichever acquires the lock first,
        # the final state must be consistent: the registration belongs either
        # to the old phase or to the new one, never to both.
        phaser    = described_class.new(2)
        can_enter = Queue.new
        phaser.arrive

        def phaser.with_lock(&block)
          synchronize(&block)
        end

        arriving = in_thread { can_enter.pop; phaser.arrive }
        register = in_thread { can_enter.pop; phaser.register }

        phaser.with_lock do
          2.times { can_enter << :go }
          # both threads are now blocked on the mutex
          wait_blocked_on_mutex([arriving, register])
          # the mutex cannot be reacquired by anyone until this block ends
        end

        [arriving, register].each(&:join)

        expect(phaser.registered_parties).to eq 3
        if phaser.phase == 1
          # arrival ran first and completed phase zero; the registration then
          # belongs entirely to phase one
          expect(phaser.unarrived_parties).to eq 3
          3.times { phaser.arrive }
          expect(phaser.phase).to eq 2
        else
          # registration ran first: it took part in phase zero, so the
          # contending arrival did not complete that phase
          expect(phaser.phase).to eq 0
          expect(phaser.unarrived_parties).to eq 2
          2.times { phaser.arrive }
          expect(phaser.phase).to eq 1
          expect(phaser.unarrived_parties).to eq 3
        end
      end

      it 'assigns a registration racing just before completion to the old phase' do
        phaser = described_class.new(2)
        phaser.arrive # one arrival left to complete phase zero

        t = in_thread do
          phaser.register
          phaser.arrive
        end
        repeat_until_success { expect(phaser.registered_parties).to eq 3 }
        # the registering party already arrived; the original last arrival is
        # still pending and is the one that must complete phase zero
        expect(phaser.phase).to eq 0
        expect(phaser.unarrived_parties).to eq 1
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.unarrived_parties).to eq 3
        expect(t.join(1)).not_to be_nil
      end

      it 'assigns a registration racing just after completion to the new phase' do
        phaser = described_class.new(2)
        phaser.arrive

        t = in_thread do
          # completes phase zero
          expect(phaser.arrive).to eq 0
          # this registration starts after completion, it must belong to phase one
          expect(phaser.register).to eq 1
        end
        expect(t.join(1)).not_to be_nil
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 3
        expect(phaser.unarrived_parties).to eq 3
      end
    end

    describe '#arrive' do

      it 'decrements the number of unarrived parties without blocking' do
        phaser = described_class.new(3)
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 0
        expect(phaser.unarrived_parties).to eq 2
        expect(phaser.arrive).to eq 0
        expect(phaser.unarrived_parties).to eq 1
      end

      it 'advances the phase when the last party arrives' do
        phaser = described_class.new(2)
        phaser.arrive
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.unarrived_parties).to eq 2
      end

      it 'raises when there is no unarrived party' do
        phaser = described_class.new(1) { false }
        phaser.arrive
        expect(phaser.phase).to eq 1
        expect { phaser.arrive }.to_not raise_error # belongs to phase one
        phaser.arrive_and_deregister
        expect(phaser.registered_parties).to eq 0
        expect { phaser.arrive }.to raise_error(IllegalOperationError)
      end

      it 'raises on a zero party phaser' do
        phaser = described_class.new(0)
        expect { phaser.arrive }.to raise_error(IllegalOperationError)
      end

      it 'returns the negative phase without changing state when terminated' do
        phaser = described_class.new(2)
        phaser.force_termination
        expect(phaser.arrive).to eq(-1)
        expect(phaser.unarrived_parties).to eq 2
      end
    end

    describe '#arrive_and_wait' do

      it 'blocks until the phase advances' do
        phaser = described_class.new(2)
        done   = CountDownLatch.new(1)

        t = in_thread do
          expect(phaser.arrive_and_wait).to eq 1
          done.count_down
        end

        t.join(0.1)
        expect(done.count).to eq 1
        expect(t.status).to eq 'sleep'

        expect(phaser.arrive_and_wait).to eq 1
        expect(done.wait(1)).to be_truthy
        expect(phaser.phase).to eq 1
      end

      it 'returns immediately when completing the phase' do
        phaser = described_class.new(1)
        expect(phaser.arrive_and_wait).to eq 1
        expect(phaser.phase).to eq 1
      end

      it 'releases every waiting party together' do
        phaser  = described_class.new(3)
        phases  = []
        started = CountDownLatch.new(2)

        threads = 2.times.map do
          in_thread do
            started.count_down
            phases << phaser.arrive_and_wait
          end
        end

        started.wait(1)
        repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }
        expect(phaser.arrive_and_wait).to eq 1
        threads.each { |t| expect(t.join(1)).not_to be_nil }
        expect(phases).to eq [1, 1]
      end

      context 'with timeout' do

        it 'returns the current phase without un-arriving on timeout' do
          phaser = described_class.new(2)
          expect(phaser.arrive_and_wait(0.05)).to eq 0
          expect(phaser.unarrived_parties).to eq 1
          expect(phaser.phase).to eq 0
        end

        it 'does not break the counts of the other parties' do
          phaser = described_class.new(2)
          in_thread { phaser.arrive_and_wait(0.05) }
          repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }
          sleep 0.15 # timeout has elapsed

          # the phase must still be completable with exactly one arrival
          expect(phaser.arrive_and_wait).to eq 1
          expect(phaser.phase).to eq 1
          expect(phaser.unarrived_parties).to eq 2
        end

        it 'returns the next phase when the other parties arrive in time' do
          phaser = described_class.new(2)
          t = in_thread { phaser.arrive_and_wait(2) }
          repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }
          expect(phaser.arrive_and_wait).to eq 1
          expect(t.join(1)).not_to be_nil
        end

        it 'returns the negative phase when terminated while waiting' do
          phaser = described_class.new(2)
          t = in_thread { phaser.arrive_and_wait }
          repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }
          phaser.force_termination
          expect(t.join(1)).not_to be_nil
          expect(phaser.phase).to eq(-1)
        end
      end

      it 'does not release parties before everybody arrived' do
        phaser  = described_class.new(3)
        done    = AtomicFixnum.new(0)

        threads = 2.times.map { in_thread { phaser.arrive_and_wait; done.increment } }
        repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }
        sleep 0.1

        expect(done.value).to eq 0
        phaser.arrive_and_wait
        threads.each { |t| expect(t.join(1)).not_to be_nil }
        expect(done.value).to eq 2
      end
    end

    describe '#arrive_and_deregister' do

      it 'removes the party for subsequent phases' do
        phaser = described_class.new(2)
        expect(phaser.arrive_and_deregister).to eq 0
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1

        phaser.arrive
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 1
        expect(phaser.unarrived_parties).to eq 1
      end

      it 'still counts the arrival for the current phase' do
        phaser = described_class.new(2)
        phaser.arrive_and_deregister
        expect(phaser.phase).to eq 0 # the other party still has to arrive
        expect(phaser.arrive).to eq 0
        expect(phaser.phase).to eq 1
        expect(phaser.registered_parties).to eq 1
      end

      it 'terminates by default when the last party leaves' do
        phaser = described_class.new(1)
        phaser.arrive_and_deregister
        expect(phaser).to be_terminated
        expect(phaser.phase).to eq(-1)
      end

      it 'raises when there is no unarrived party' do
        phaser = described_class.new(0)
        expect { phaser.arrive_and_deregister }.to raise_error(IllegalOperationError)
      end

      it 'has no effect when terminated' do
        phaser = described_class.new(1)
        phaser.force_termination
        expect(phaser.arrive_and_deregister).to eq(-1)
        expect(phaser.registered_parties).to eq 1
      end
    end

    describe '#await_advance' do

      it 'waits for the given phase without arriving' do
        phaser = described_class.new(1)
        t = in_thread { phaser.await_advance(0) }
        t.join(0.1)
        expect(t.status).to eq 'sleep'
        phaser.arrive
        expect(t.join(1)).not_to be_nil
        expect(phaser.phase).to eq 1
      end

      it 'returns immediately when awaiting an earlier phase' do
        phaser = described_class.new(1)
        phaser.arrive_and_wait
        expect(phaser.phase).to eq 1
        expect(phaser.await_advance(0)).to eq 1
      end

      it 'returns immediately on a zero party phaser' do
        phaser = described_class.new(0)
        expect(phaser.await_advance(0)).to eq 0
      end

      it 'defaults to awaiting the current phase' do
        phaser = described_class.new(1)
        t = in_thread { phaser.await_advance }
        t.join(0.1)
        expect(t.status).to eq 'sleep'
        phaser.arrive
        expect(t.join(1)).not_to be_nil
      end

      it 'only ends the current wait on timeout' do
        phaser = described_class.new(1)
        expect(phaser.await_advance(0, 0.05)).to eq 0
        expect(phaser.phase).to eq 0

        # the phaser is still usable and advances with the single arrival
        phaser.arrive
        expect(phaser.phase).to eq 1
      end

      it 'returns the negative termination phase' do
        phaser = described_class.new(1)
        t = in_thread { phaser.await_advance }
        t.join(0.1)
        phaser.force_termination
        expect(t.join(1)).not_to be_nil
        expect(phaser.phase).to eq(-1)
      end
    end

    describe '#force_termination' do

      it 'sets a negative phase and releases waiting parties' do
        phaser = described_class.new(2)
        t1 = in_thread { phaser.arrive_and_wait }
        t2 = in_thread { phaser.await_advance }
        repeat_until_success { expect(phaser.unarrived_parties).to eq 1 }

        expect(phaser.force_termination).to eq(-1)
        [t1, t2].each { |t| expect(t.join(1)).not_to be_nil }
        expect(phaser).to be_terminated
        expect(phaser.phase).to eq(-1)
      end

      it 'is idempotent' do
        phaser = described_class.new(2)
        phaser.arrive; phaser.arrive
        expect(phaser.phase).to eq 1
        expect(phaser.force_termination).to eq(-2)
        expect(phaser.force_termination).to eq(-2)
      end

      it 'keeps the registered and unarrived counts' do
        phaser = described_class.new(3)
        phaser.arrive
        phaser.force_termination
        expect(phaser.registered_parties).to eq 3
        expect(phaser.unarrived_parties).to eq 2
      end

      it 'makes further arrivals and waits return immediately' do
        phaser = described_class.new(2)
        phaser.force_termination
        expect(phaser.arrive).to eq(-1)
        expect(phaser.arrive_and_wait).to eq(-1)
        expect(phaser.arrive_and_deregister).to eq(-1)
        expect(phaser.await_advance(0)).to eq(-1)
      end
    end

    describe 'on_advance block' do

      it 'is called with the finished phase and registered parties' do
        seen = []
        phaser = described_class.new(1) { |phase, parties| seen << [phase, parties]; false }
        phaser.arrive
        phaser.arrive
        expect(seen).to eq [[0, 1], [1, 1]]
      end

      it 'terminates the phaser when the block returns truthy' do
        phaser = described_class.new(1) { |phase, _| phase >= 1 }
        phaser.arrive_and_wait
        expect(phaser.phase).to eq 1
        phaser.arrive_and_wait
        expect(phaser.phase).to eq(-2)
        expect(phaser).to be_terminated
      end

      it 'sees the parties remaining after a deregistration before continuing' do
        phaser = described_class.new(2) { |_, parties| @last = parties; false }
        phaser.arrive_and_deregister
        phaser.arrive
        expect(@last).to eq 1
        expect(phaser.registered_parties).to eq 1
        expect(phaser.phase).to eq 1
      end

      it 'does not advance the phase when the block raises and keeps the phaser usable' do
        phaser = described_class.new(1) { raise 'boom' }
        expect { phaser.arrive }.to raise_error(RuntimeError, 'boom')
        expect(phaser.phase).to eq 0
        expect(phaser.unarrived_parties).to eq 1
        phaser = described_class.new(1)
        phaser.arrive
        expect(phaser.phase).to eq 1
      end
    end

    describe 'multiple rounds with a changing number of parties' do

      it 'supports parties joining and leaving between phases' do
        phaser = described_class.new(1)

        2.times { phaser.register }
        expect(phaser.registered_parties).to eq 3

        # round one with three parties
        3.times { phaser.arrive }
        expect(phaser.phase).to eq 1

        # one party leaves before round two, nobody joins
        phaser.arrive_and_deregister
        expect(phaser.registered_parties).to eq 2
        2.times { phaser.arrive }
        expect(phaser.phase).to eq 2
        expect(phaser.registered_parties).to eq 2
      end

      it 'coordinates several worker threads across rounds' do
        phaser  = described_class.new(1)
        rounds  = 5
        workers = 4
        results = Array.new(workers) { [] }
        ready   = CountDownLatch.new(workers)

        threads = workers.times.map do |i|
          in_thread do
            phaser.register
            ready.count_down
            rounds.times do |round|
              results[i] << round
              phaser.arrive_and_wait
            end
            phaser.arrive_and_deregister
          end
        end

        ready.wait(1)
        rounds.times { phaser.arrive_and_wait }
        phaser.arrive_and_deregister

        threads.each { |t| expect(t.join(5)).not_to be_nil }
        expect(results).to all(eq((0...rounds).to_a))
        expect(phaser).to be_terminated
      end
    end

    # blocks until all given threads have started and are blocked trying to
    # acquire the phaser mutex
    def wait_blocked_on_mutex(threads)
      deadline = Concurrent.monotonic_time + 2
      while threads.any? { |t| t.status != 'sleep' } && Concurrent.monotonic_time < deadline
        Thread.pass
      end
      # give the sleeping threads a moment to reach the mutex
      sleep 0.05
    end
  end
end
