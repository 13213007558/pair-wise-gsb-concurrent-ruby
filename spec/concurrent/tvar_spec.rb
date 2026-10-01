require 'concurrent/tvar'
require 'concurrent/atomic/cyclic_barrier'

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

  RSpec.describe '#retry_transaction' do

    it 'raises an exception outside an #atomically block' do
      expect { Concurrent::retry_transaction }.to raise_error(Concurrent::Transaction::RetryError)
    end

    it 'raises ArgumentError when the read set is empty and no timeout is given' do
      expect {
        Concurrent::atomically { Concurrent::retry_transaction }
      }.to raise_error(ArgumentError)
    end

    it 'raises RetryTimeoutError when an empty read set retry reaches its timeout' do
      started = Concurrent.monotonic_time

      expect {
        Concurrent::atomically(0.1) { Concurrent::retry_transaction }
      }.to raise_error(Concurrent::Transaction::RetryTimeoutError)

      expect(Concurrent.monotonic_time - started).to be >= 0.1
    end

    it 'raises RetryTimeoutError when no relevant TVar changes within the timeout' do
      t = TVar.new(0)

      expect {
        Concurrent::atomically(0.1) do
          Concurrent::retry_transaction if t.value.zero?
        end
      }.to raise_error(Concurrent::Transaction::RetryTimeoutError)
    end

    it 'sleeps instead of busy-waiting, ignores unrelated writes, and commits once a read TVar changes' do
      relevant = TVar.new(0)
      unrelated = TVar.new(0)
      entered = CyclicBarrier.new(2)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          entered.wait if runs == 1
          value = relevant.value
          Concurrent::retry_transaction if value.zero?
          value
        end
      end

      # The transaction has checked its condition and is about to sleep.
      entered.wait

      # A write to a TVar the transaction did not read must not wake it up.
      unrelated.value = 1
      sleep 0.3
      expect(runs).to eq 1

      # A write to a TVar in the read set wakes it up exactly once and the
      # transaction commits. The write blocks on the TVar lock until the
      # sleeping transaction has registered its waiter and released its
      # locks, so it cannot be lost between the check and the sleep.
      relevant.value = 1

      expect(thread.join(5)).not_to be_nil
      expect(thread.value).to eq 1
      expect(runs).to eq 2
      expect(relevant.value).to eq 1
    end

    it 'releases its locks while sleeping' do
      t = TVar.new(0)
      entered = CyclicBarrier.new(2)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          entered.wait if runs == 1
          Concurrent::retry_transaction if t.value.zero?
        end
      end

      entered.wait

      # If the sleeping transaction still held its locks this write could
      # never commit and the join below would time out.
      t.value = 42

      expect(thread.join(5)).not_to be_nil
      expect(t.value).to eq 42
    end

  end

  RSpec.describe '#or_else' do

    it 'runs the second branch when the first one retries' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { Concurrent::retry_transaction },
          -> { :second }
        )
      end

      expect(result).to eq :second
    end

    it 'returns the result of the first branch when it does not retry' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { :first },
          -> { :second }
        )
      end

      expect(result).to eq :first
    end

    it 'rolls back writes made by the failed branch' do
      t = TVar.new(0)

      Concurrent::atomically do
        Concurrent::or_else(
          -> { t.value = 1; Concurrent::retry_transaction },
          -> { :fallback }
        )
      end

      expect(t.value).to eq 0
    end

    it 'restores pre-branch writes when the branch retries' do
      t = TVar.new(0)

      result = Concurrent::atomically do
        t.value = 1
        Concurrent::or_else(
          -> { t.value = 2; Concurrent::retry_transaction },
          -> { t.value }
        )
      end

      expect(result).to eq 1
      expect(t.value).to eq 1
    end

    it 'waits on the read sets of all failed branches' do
      t1 = TVar.new(0)
      t2 = TVar.new(0)
      entered = CyclicBarrier.new(2)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          entered.wait if runs == 1
          Concurrent::or_else(
            -> { Concurrent::retry_transaction if t1.value.zero?; :one },
            -> { Concurrent::retry_transaction if t2.value.zero?; :two }
          )
        end
      end

      entered.wait

      # Both branches retried, so the transaction sleeps. Changing a TVar
      # that was only read by the failed first branch must still wake it.
      t1.value = 1

      expect(thread.join(5)).not_to be_nil
      expect(thread.value).to eq :one
      expect(runs).to eq 2
    end

    it 'wakes up when the second branch read set changes' do
      t1 = TVar.new(0)
      t2 = TVar.new(0)
      entered = CyclicBarrier.new(2)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          entered.wait if runs == 1
          Concurrent::or_else(
            -> { Concurrent::retry_transaction if t1.value.zero?; :one },
            -> { Concurrent::retry_transaction if t2.value.zero?; :two }
          )
        end
      end

      entered.wait
      t2.value = 1

      expect(thread.join(5)).not_to be_nil
      expect(thread.value).to eq :two
    end

    it 'nests' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> {
            Concurrent::or_else(
              -> { Concurrent::retry_transaction },
              -> { Concurrent::retry_transaction }
            )
          },
          -> { :outer_fallback }
        )
      end

      expect(result).to eq :outer_fallback
    end

    it 'wraps itself in a transaction when called outside #atomically' do
      result = Concurrent::or_else(
        -> { Concurrent::retry_transaction },
        -> { :ok }
      )

      expect(result).to eq :ok
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

end
