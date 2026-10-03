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

  # A TVar that lets a test hook run right after a retrying transaction has
  # registered its waiter, but before the transaction releases its locks and
  # goes to sleep.
  class TVarWithWaiterHook < TVar
    attr_accessor :on_add_waiter

    def add_waiter(waiter)
      super
      on_add_waiter.call if on_add_waiter
    end
  end

  RSpec.describe '#retry_transaction' do

    it 'raises an exception outside an #atomically block' do
      expect { Concurrent::retry_transaction }.to raise_error(Concurrent::Transaction::RetryError)
    end

    it 'raises when the transaction has read nothing, as it could never be woken' do
      expect {
        Concurrent::atomically { Concurrent::retry_transaction }
      }.to raise_error(Concurrent::Transaction::RetryError, /empty read set/)
    end

    it 'raises when the transaction only wrote TVars, and discards the writes' do
      t = TVar.new(0)

      expect {
        Concurrent::atomically do
          t.value = 1
          Concurrent::retry_transaction
        end
      }.to raise_error(Concurrent::Transaction::RetryError, /empty read set/)

      expect(t.value).to eq 0
    end

    it 'sleeps without re-running until a TVar read by the transaction changes' do
      t = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          value = t.value
          Concurrent::retry_transaction if value.zero?
          value
        end
      end

      sleep 0.2
      expect(runs).to eq 1
      expect(thread.status).to eq 'sleep'

      t.value = 42

      expect(thread.join(5)).to eq thread
      expect(thread.value).to eq 42
      expect(runs).to eq 2
    end

    it 're-runs the whole transaction from the start after waking' do
      t = TVar.new(0)
      seen = []

      thread = Thread.new do
        Concurrent::atomically do
          seen << t.value
          Concurrent::retry_transaction if t.value < 2
        end
      end

      sleep 0.2
      t.value = 1
      sleep 0.2
      t.value = 2

      expect(thread.join(5)).to eq thread
      expect(seen).to eq [0, 1, 2]
    end

    it 'discards writes made before retrying' do
      a = TVar.new(0)
      b = TVar.new(0)

      thread = Thread.new do
        Concurrent::atomically do
          b.value = 99
          Concurrent::retry_transaction if a.value.zero?
        end
      end

      sleep 0.2
      expect(b.value).to eq 0

      a.value = 1
      expect(thread.join(5)).to eq thread
      expect(b.value).to eq 99
    end

    it 'does not wake when a TVar the transaction did not read changes' do
      a = TVar.new(0)
      b = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::retry_transaction if a.value.zero?
        end
      end

      sleep 0.2
      b.value = 1
      sleep 0.2
      expect(runs).to eq 1

      a.value = 1
      expect(thread.join(5)).to eq thread
    end

    it 'retries the outermost transaction from nested atomically blocks' do
      t = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::atomically do
            Concurrent::retry_transaction if t.value.zero?
          end
          t.value
        end
      end

      sleep 0.2
      expect(runs).to eq 1

      t.value = 3
      expect(thread.join(5)).to eq thread
      expect(thread.value).to eq 3
    end

    it 'treats a signal sent before the transaction starts waiting as a wakeup' do
      waiter = Concurrent::Transaction::RetryWaiter.new
      waiter.signal

      thread = Thread.new { waiter.wait }
      expect(thread.join(5)).to eq thread
    end

    it 'does not miss a change that lands while the transaction is preparing to sleep' do
      20.times do
        t = TVarWithWaiterHook.new(0)

        # The transaction has registered its waiter and is about to release
        # its locks and sleep. Commit a change right into that gap: the
        # writer spins until the locks are released, then commits and
        # signals, possibly before the transaction starts waiting.
        t.on_add_waiter = proc do
          Thread.new { t.value = 1 }
        end

        thread = Thread.new do
          Concurrent::atomically do
            Concurrent::retry_transaction if t.value.zero?
          end
        end

        expect(thread.join(10)).to eq thread
      end
    end

  end

  RSpec.describe '#or_else' do

    it 'raises an exception outside an #atomically block' do
      expect {
        Concurrent::or_else(-> { }, -> { })
      }.to raise_error(ArgumentError, /atomically/)
    end

    it 'raises unless both alternatives are callable' do
      expect {
        Concurrent::atomically { Concurrent::or_else(1, 2) }
      }.to raise_error(ArgumentError, /callable/)
    end

    it 'returns the left alternative when it succeeds' do
      right_ran = false

      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { :left },
          -> { right_ran = true; :right }
        )
      end

      expect(result).to eq :left
      expect(right_ran).to eq false
    end

    it 'runs the right alternative when the left retries' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { Concurrent::retry_transaction },
          -> { :right }
        )
      end

      expect(result).to eq :right
    end

    it 'rolls back the left alternative writes when it retries' do
      t = TVar.new(0)

      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { t.value = 99; Concurrent::retry_transaction },
          -> { :right }
        )
      end

      expect(result).to eq :right
      expect(t.value).to eq 0
    end

    it 'lets the right alternative see committed values, not the left writes' do
      t = TVar.new(0)

      seen = Concurrent::atomically do
        Concurrent::or_else(
          -> { t.value = 99; Concurrent::retry_transaction },
          -> { t.value }
        )
      end

      expect(seen).to eq 0
    end

    it 'undoes left writes to TVars opened before or_else' do
      t = TVar.new(0)

      result = Concurrent::atomically do
        t.value = 1
        seen = Concurrent::or_else(
          -> { t.value = 99; Concurrent::retry_transaction },
          -> { t.value }
        )
        [seen, t.value]
      end

      expect(result).to eq [1, 1]
      expect(t.value).to eq 1
    end

    it 'commits the right alternative writes when the left retries' do
      t = TVar.new(0)

      Concurrent::atomically do
        Concurrent::or_else(
          -> { Concurrent::retry_transaction },
          -> { t.value = 2 }
        )
      end

      expect(t.value).to eq 2
    end

    it 'nests or_else alternatives' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> {
            Concurrent::or_else(
              -> { Concurrent::retry_transaction },
              -> { Concurrent::retry_transaction }
            )
          },
          -> { :outer_right }
        )
      end

      expect(result).to eq :outer_right
    end

    it 'lets exceptions other than retry abort the whole transaction' do
      t = TVar.new(0)

      expect {
        Concurrent::atomically do
          Concurrent::or_else(
            -> { t.value = 1; raise StandardError, 'boom' },
            -> { t.value = 2 }
          )
        end
      }.to raise_error(StandardError, 'boom')

      expect(t.value).to eq 0
    end

    it 'does not catch abort_transaction in the left alternative' do
      count = 0

      Concurrent::atomically do
        if count == 0
          count = 1
          Concurrent::or_else(
            -> { Concurrent::abort_transaction },
            -> { }
          )
        else
          count = 2
        end
      end

      expect(count).to eq 2
    end

    it 'waits on the union of both alternatives read sets when both retry' do
      a = TVar.new(0)
      b = TVar.new(0)
      c = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::or_else(
            -> {
              a.value
              b.value = 99
              Concurrent::retry_transaction if a.value.zero?
              :left
            },
            -> {
              c.value
              Concurrent::retry_transaction if c.value.zero? && a.value.zero?
              :right
            }
          )
        end
      end

      # Both alternatives retried: the left's write to b must not be visible
      # and the transaction sleeps on the union {a, c}.
      sleep 0.2
      expect(runs).to eq 1
      expect(b.value).to eq 0

      # b was only written by the failed left alternative, never read, so
      # changing it must not wake the transaction.
      b.value = 5
      sleep 0.2
      expect(runs).to eq 1

      # Changing c (read by the right alternative) wakes the transaction;
      # the right alternative now succeeds and the left's write stays undone.
      c.value = 1
      expect(thread.join(5)).to eq thread
      expect(thread.value).to eq :right
      expect(b.value).to eq 5
      expect(runs).to eq 2
    end

    it 'wakes when a TVar read by the left alternative changes' do
      a = TVar.new(0)
      c = TVar.new(0)

      thread = Thread.new do
        Concurrent::atomically do
          Concurrent::or_else(
            -> { Concurrent::retry_transaction if a.value.zero?; :left },
            -> { Concurrent::retry_transaction if c.value.zero?; :right }
          )
        end
      end

      sleep 0.2
      a.value = 7

      expect(thread.join(5)).to eq thread
      expect(thread.value).to eq :left
    end

  end

end
