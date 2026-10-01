require 'concurrent/tvar'

module Concurrent

  RSpec.describe TVar do

    context '#initialize' do

      it 'accepts an initial value' do
        t = TVar.new(14)
        expect(t.value).to eq 14
      end

    end

    context '#value' do

      it 'gets the value' do
        t = TVar.new(14)
        expect(t.value).to eq 14
      end

    end

    context '#value=' do

      it 'sets the value' do
        t = TVar.new(14)
        t.value = 2
        expect(t.value).to eq 2
      end

    end

  end

  RSpec.describe '#atomically' do

    it 'raises an exception when no block given' do
      expect { Concurrent::atomically }.to raise_error(ArgumentError)
    end

    it 'raises the same exception that was raised in Concurrent::atomically' do
      expect {
        Concurrent::atomically do
          raise StandardError, 'This is an error!'
        end
      }.to raise_error(StandardError, 'This is an error!')
    end

    it 'aborts and releases locks for exceptions outside StandardError' do
      t = TVar.new(0)
      error = Interrupt.new('This is an interrupt!')

      expect {
        Concurrent::atomically do
          t.value = 1
          raise error
        end
      }.to raise_error(error)

      expect(t.value).to eq 0
      expect { Concurrent::atomically { t.value = 2 } }.not_to raise_error
      expect(t.value).to eq 2
    end

    it 'retries on abort' do
      count = 0

      Concurrent::atomically do
        if count == 0
          count = 1
          Concurrent::abort_transaction
        else
          count = 2
        end
      end

      expect(count).to eq 2
    end

    it 'commits writes if the transaction succeeds' do
      t = TVar.new(0)

      Concurrent::atomically do
        t.value = 1
      end

      expect(t.value).to eq 1
    end

    it 'undoes writes if the transaction is aborted' do
      t = TVar.new(0)

      count = 0

      Concurrent::atomically do
        if count == 0
          t.value = 1
          count = 1
          Concurrent::abort_transaction
        end
      end

      expect(t.value).to eq 0
    end

    it 'provides atomicity' do
      t1 = TVar.new(0)
      t2 = TVar.new(0)

      count = 0

      Concurrent::atomically do
        if count == 0
          count = 1
          t1.value = 1
          Concurrent::abort_transaction
          t2.value = 2
        end
      end

      expect(t1.value).to eq 0
      expect(t2.value).to eq 0
    end

    it 'nests' do
      t = TVar.new(0)

      Concurrent::atomically do
        expect(t.value).to eq 0
        t.value = 1
        Concurrent::atomically do
          expect(t.value).to eq 1
          t.value = 2
          Concurrent::atomically do
            expect(t.value).to eq 2
            t.value = 3
          end
          expect(t.value).to eq 3
          t.value = 4
        end
        expect(t.value).to eq 4
        t.value = 5
      end

      expect(t.value).to eq 5
    end

    it 'reflects transactional writes from within the same transaction' do
      t = TVar.new(0)

      Concurrent::atomically do
        expect(t.value).to eq 0
        t.value = 14
        expect(t.value).to eq 14
        t.value = 2
        expect(t.value).to eq 2
      end
    end

  end

  RSpec.describe '#abort_transaction' do

    it 'raises an exception outside an #atomically block' do
      expect { Concurrent::abort_transaction }.to raise_error(Concurrent::Transaction::AbortError)
    end

  end

  RSpec.describe '#leave_transaction' do

    it 'raises an exception outside an #atomically block' do
      expect { Concurrent::leave_transaction }.to raise_error(Concurrent::Transaction::LeaveError)
    end

    it 'neither commits nor aborts a transaction' do
      t = TVar.new(0)

      Concurrent::atomically do
        expect(t.value).to eq 0
        t.value = 14
        Concurrent::leave_transaction
      end

      expect(t.value).to eq 0
    end

  end

  RSpec.describe '#retry_transaction' do

    it 'raises an exception outside an #atomically block' do
      expect { Concurrent::retry_transaction }.to raise_error(Concurrent::Transaction::RetryError)
    end

    it 'sleeps until a TVar in its read set changes instead of busy-waiting' do
      gate = TVar.new(false)
      unrelated = TVar.new(0)
      runs = 0
      read_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          value = gate.value
          # Barrier: the transaction has read `gate` (and therefore holds
          # its lock) and is about to go to sleep.
          read_done << true if runs == 1
          Concurrent::retry_transaction unless value
          value
        end
      end

      read_done.pop

      # Writes to TVars the transaction has not read must not wake it up.
      10.times { |i| unrelated.value = i }
      sleep 0.1
      expect(runs).to eq 1

      # A write to a TVar in the read set wakes the transaction, which
      # then runs exactly once more and commits.
      gate.value = true

      expect(thread.join(5)).to_not be_nil
      expect(thread.value).to eq true
      expect(runs).to eq 2
    end

    it 'does not miss a write that happens while it is going to sleep' do
      gate = TVar.new(0)
      read_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          value = gate.value
          # The transaction holds the lock of `gate` from the read until
          # it has registered itself as a waiter, so the write below can
          # only commit after the waiter is in place.
          read_done << true
          Concurrent::retry_transaction if value < 5
          value
        end
      end

      read_done.pop
      5.times { gate.value = gate.value + 1 }

      expect(thread.join(5)).to_not be_nil
      expect(thread.value).to eq 5
    end

    it 'retries the whole transaction from within a nested atomically' do
      gate = TVar.new(false)
      runs = 0
      read_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::atomically do
            value = gate.value
            read_done << true if runs == 1
            Concurrent::retry_transaction unless value
          end
        end
      end

      read_done.pop
      gate.value = true

      expect(thread.join(5)).to_not be_nil
      expect(runs).to eq 2
    end

    it 'raises Concurrent::TimeoutError when the wait outlasts the timeout' do
      gate = TVar.new(false)

      expect {
        Concurrent::atomically(timeout: 0.1) do
          Concurrent::retry_transaction unless gate.value
        end
      }.to raise_error(Concurrent::TimeoutError)
    end

    it 'commits normally when woken before the timeout' do
      gate = TVar.new(false)
      read_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically(timeout: 5) do
          value = gate.value
          read_done << true
          Concurrent::retry_transaction unless value
          value
        end
      end

      read_done.pop
      gate.value = true

      expect(thread.join(5)).to_not be_nil
      expect(thread.value).to eq true
    end

    it 'raises when retrying with an empty read set and no timeout' do
      expect {
        Concurrent::atomically { Concurrent::retry_transaction }
      }.to raise_error(Concurrent::Transaction::InvalidRetryError)
    end

    it 'sleeps out the timeout when retrying with an empty read set' do
      started = Concurrent.monotonic_time

      expect {
        Concurrent::atomically(timeout: 0.1) { Concurrent::retry_transaction }
      }.to raise_error(Concurrent::TimeoutError)

      expect(Concurrent.monotonic_time - started).to be >= 0.1
    end

  end

  RSpec.describe '#or_else' do

    it 'raises an exception outside an #atomically block' do
      expect {
        Concurrent::or_else(-> {}, -> {})
      }.to raise_error(ArgumentError)
    end

    it 'returns the result of the first branch when it succeeds' do
      t = TVar.new(1)

      result = Concurrent::atomically do
        Concurrent::or_else(-> { t.value }, -> { 2 })
      end

      expect(result).to eq 1
    end

    it 'runs the second branch when the first one retries' do
      t = TVar.new(2)

      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { Concurrent::retry_transaction },
          -> { t.value }
        )
      end

      expect(result).to eq 2
    end

    it 'rolls back the writes of a branch that retries' do
      a = TVar.new(0)
      b = TVar.new(0)

      Concurrent::atomically do
        Concurrent::or_else(
          -> { a.value = 1; Concurrent::retry_transaction },
          -> { b.value = 2 }
        )
      end

      expect(a.value).to eq 0
      expect(b.value).to eq 2
    end

    it 'releases the locks acquired by a branch that retries' do
      a = TVar.new(0)
      b = TVar.new(0)
      entered = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          Concurrent::or_else(
            -> { a.value; entered << true; Concurrent::retry_transaction },
            -> { b.value }
          )
        end
      end

      entered.pop
      # `a` was only touched by the failed branch - its lock must be free.
      a.value = 42
      expect(a.value).to eq 42

      expect(thread.join(5)).to_not be_nil
    end

    it 'sleeps on the union of both read sets when both branches retry' do
      first_gate = TVar.new(false)
      second_gate = TVar.new(false)
      unrelated = TVar.new(0)
      runs = 0
      branches_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::or_else(
            -> {
              value = first_gate.value
              Concurrent::retry_transaction unless value
              :first
            },
            -> {
              value = second_gate.value
              # Barrier: both branches have run and retried, so the
              # transaction is about to sleep on both read sets.
              branches_done << true if runs == 1
              Concurrent::retry_transaction unless value
              :second
            }
          )
        end
      end

      branches_done.pop

      # Unrelated writes must not wake the transaction.
      unrelated.value = 1
      sleep 0.1
      expect(runs).to eq 1

      # A TVar read only by the second branch wakes the whole transaction.
      second_gate.value = true

      expect(thread.join(5)).to_not be_nil
      expect(thread.value).to eq :second
      expect(runs).to eq 2
    end

    it 'is also woken by TVars read only in the first branch' do
      first_gate = TVar.new(false)
      second_gate = TVar.new(false)
      branches_done = Queue.new

      thread = Thread.new do
        Concurrent::atomically do
          Concurrent::or_else(
            -> {
              value = first_gate.value
              Concurrent::retry_transaction unless value
              :first
            },
            -> {
              value = second_gate.value
              branches_done << true
              Concurrent::retry_transaction unless value
              :second
            }
          )
        end
      end

      branches_done.pop
      sleep 0.1
      first_gate.value = true

      expect(thread.join(5)).to_not be_nil
      expect(thread.value).to eq :first
    end

    it 'does not catch abort_transaction raised by a branch' do
      t = TVar.new(0)
      count = 0

      Concurrent::atomically do
        count += 1
        if count == 1
          Concurrent::or_else(
            -> { Concurrent::abort_transaction },
            -> { t.value = 1 }
          )
        end
      end

      expect(count).to eq 2
      expect(t.value).to eq 0
    end

    it 'works when nested inside another or_else branch' do
      t = TVar.new(0)

      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { t.value; Concurrent::retry_transaction },
          -> {
            Concurrent::or_else(
              -> { Concurrent::retry_transaction },
              -> { :inner }
            )
          }
        )
      end

      expect(result).to eq :inner
    end

  end

end
