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

    it 'raises when the transaction has read nothing, as it could never be woken' do
      expect {
        Concurrent::atomically { Concurrent::retry_transaction }
      }.to raise_error(Concurrent::Transaction::RetryError)
    end

    it 'sleeps until a TVar it read is changed, then re-runs the transaction' do
      balance = TVar.new(0)

      thread = Thread.new do
        Concurrent::atomically do
          Concurrent::retry_transaction if balance.value < 10
          balance.value
        end
      end

      sleep 0.1
      expect(thread).to be_alive

      balance.value = 5 # wakes the transaction, but it retries again
      sleep 0.1
      expect(thread).to be_alive

      balance.value = 10
      expect(thread.value).to eq 10
    end

    it 'does not busy-spin while waiting' do
      balance = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::retry_transaction if balance.value < 10
        end
      end

      sleep 0.2
      balance.value = 10
      expect(thread.join(5)).not_to be_nil
      expect(runs).to eq 2
    end

    it 'does not miss a change that happens just as it is going to sleep' do
      50.times do
        t = TVar.new(0)
        read = Queue.new

        sleeper = Thread.new do
          Concurrent::atomically do
            value = t.value
            read << true
            Concurrent::retry_transaction if value == 0
            value
          end
        end

        read.pop
        t.value = 1

        expect(sleeper.join(5)).not_to be_nil
        expect(sleeper.value).to eq 1
      end
    end

    it 're-runs the whole transaction when a nested atomically retries' do
      t = TVar.new(0)
      runs = 0

      thread = Thread.new do
        Concurrent::atomically do
          runs += 1
          Concurrent::atomically do
            Concurrent::retry_transaction if t.value == 0
          end
        end
      end

      sleep 0.1
      t.value = 1
      expect(thread.join(5)).not_to be_nil
      expect(runs).to eq 2
    end

    it 'aborts and propagates exceptions raised before a retry is woken' do
      t = TVar.new(0)

      expect {
        Concurrent::atomically do
          t.value = 1
          raise StandardError, 'This is an error!'
        end
      }.to raise_error(StandardError, 'This is an error!')

      expect(t.value).to eq 0
    end

  end

  RSpec.describe '#or_else' do

    it 'requires two branches' do
      expect {
        Concurrent::atomically { Concurrent::or_else(-> { :left }) }
      }.to raise_error(ArgumentError)
    end

    it 'returns the result of the left branch when it succeeds' do
      ran_right = false

      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { :left },
          -> { ran_right = true; :right }
        )
      end

      expect(result).to eq :left
      expect(ran_right).to be false
    end

    it 'runs the right branch when the left branch retries' do
      result = Concurrent::atomically do
        Concurrent::or_else(
          -> { Concurrent::retry_transaction },
          -> { :right }
        )
      end

      expect(result).to eq :right
    end

    it 'accepts a block as the right branch' do
      result = Concurrent::atomically do
        Concurrent::or_else(-> { Concurrent::retry_transaction }) { :right }
      end

      expect(result).to eq :right
    end

    it 'wraps itself in a transaction when called outside #atomically' do
      result = Concurrent::or_else(
        -> { Concurrent::retry_transaction },
        -> { :right }
      )

      expect(result).to eq :right
    end

    it 'discards writes made by the left branch when it retries' do
      a = TVar.new(0)
      b = TVar.new(0)
      c = TVar.new(0)

      sleeper = Thread.new do
        Concurrent::atomically do
          Concurrent::or_else(
            -> {
              if a.value == 0
                b.value = 1
                Concurrent::retry_transaction
              end
            },
            -> { Concurrent::retry_transaction if c.value == 0 }
          )
        end
      end

      sleep 0.1
      expect(b.value).to eq 0 # the left branch write was rolled back

      # the transaction only read a and c, so writing b must not wake it
      b.value = 5
      sleep 0.1
      expect(sleeper).to be_alive
      expect(b.value).to eq 5

      # both branches retried, so it sleeps on the union of their read sets
      c.value = 1
      expect(sleeper.join(5)).not_to be_nil

      # the re-run left branch retried again, so its write never committed
      expect(b.value).to eq 5
    end

    it 'sleeps on the union of both read sets when both branches retry' do
      a = TVar.new(0)
      c = TVar.new(0)

      sleeper = Thread.new do
        Concurrent::atomically do
          Concurrent::or_else(
            -> { Concurrent::retry_transaction if a.value == 0 },
            -> { Concurrent::retry_transaction if c.value == 0 }
          )
        end
      end

      sleep 0.1
      expect(sleeper).to be_alive

      a.value = 1 # wakes the transaction through the left branch read set
      expect(sleeper.join(5)).not_to be_nil
    end

    it 'does not catch exceptions other than retry' do
      b = TVar.new(0)

      expect {
        Concurrent::atomically do
          Concurrent::or_else(
            -> { b.value = 1; raise StandardError, 'This is an error!' },
            -> { :right }
          )
        end
      }.to raise_error(StandardError, 'This is an error!')

      expect(b.value).to eq 0
    end

    it 'does not catch abort_transaction' do
      count = 0

      Concurrent::atomically do
        if count == 0
          count = 1
          Concurrent::or_else(
            -> { Concurrent::abort_transaction },
            -> { :right }
          )
        end
      end

      expect(count).to eq 1
    end

  end

end
