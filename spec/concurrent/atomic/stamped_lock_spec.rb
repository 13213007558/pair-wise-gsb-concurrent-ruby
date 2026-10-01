require 'concurrent/atomic/stamped_lock'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/atomic_boolean'

module Concurrent

  RSpec.describe StampedLock do

    context '#try_optimistic_read and #validate' do

      it 'returns a valid stamp when no lock is held' do
        stamp = subject.try_optimistic_read
        expect(stamp).to be > 0
        expect(subject.validate(stamp)).to be true
      end

      it 'returns zero while the write lock is held' do
        ws = subject.acquire_write_lock
        expect(subject.try_optimistic_read).to eq 0
        subject.release_write_lock(ws)
      end

      it 'validates zero, negative, nil and arbitrary stamps as false' do
        expect(subject.validate(0)).to be false
        expect(subject.validate(-1)).to be false
        expect(subject.validate(nil)).to be false
        expect(subject.validate('stamp')).to be false
      end

      it 'does not accept a read or write stamp for validation' do
        read_stamp = subject.acquire_read_lock
        subject.release_read_lock(read_stamp)
        # read stamps are not optimistic stamps
        expect(subject.validate(read_stamp)).to be false
        ws = subject.acquire_write_lock
        expect(subject.validate(ws)).to be false
        subject.release_write_lock(ws)
      end

      it 'keeps a stamp valid when only read locks come and go' do
        stamp = subject.try_optimistic_read
        rs = subject.acquire_read_lock
        subject.release_read_lock(rs)
        expect(subject.validate(stamp)).to be true
      end
    end

    context 'optimistic stamp invalidation by writes' do

      it 'invalidates a stamp once a write cycle completes (deterministic interleaving)' do
        data = { value: 1 }
        lock = subject

        # Reader side: take optimistic stamp and observe the OLD value.
        stamp = lock.try_optimistic_read
        seen  = data[:value]
        expect(seen).to eq 1 # the reader really saw the old value

        # Writer side: the complete locked write interleaves between the
        # reader's read and its validation.
        ws = lock.acquire_write_lock
        data[:value] = 2
        lock.release_write_lock(ws)

        # The old value was observed, so validation MUST fail: it is
        # impossible here for a stale read to come with a passing stamp.
        expect(lock.validate(stamp)).to be false

        # Prescribed fallback: re-read under a pessimistic read lock.
        rs = lock.acquire_read_lock
        begin
          fresh = data[:value]
        ensure
          lock.release_read_lock(rs)
        end
        expect(fresh).to eq 2
      end

      it 'invalidates a stamp while a write is still in progress' do
        lock = subject
        entered = Concurrent::CountDownLatch.new(1)
        release = Concurrent::CountDownLatch.new(1)
        stamp = nil
        seen  = nil

        writer = in_thread do
          ws = lock.acquire_write_lock
          entered.count_down
          release.wait(1)
          lock.release_write_lock(ws)
        end

        # Take the optimistic stamp before the writer acquires.
        stamp = lock.try_optimistic_read
        expect(stamp).to be > 0
        expect(entered.wait(1)).to be true

        # Writer holds the lock: optimistic reads are refused...
        expect(lock.try_optimistic_read).to eq 0
        # ...and the old stamp no longer validates.
        expect(lock.validate(stamp)).to be false

        release.count_down
        writer.join

        # It stays invalid after the writer has finished as well.
        expect(lock.validate(stamp)).to be false
      end

      it 'never reports "old value seen but stamp valid" across many forced interleavings' do
        data = Concurrent::AtomicBoolean.new(false) # false = old value
        lock = subject
        violations = Concurrent::AtomicFixnum.new(0)
        fallback_used = Concurrent::AtomicFixnum.new(0)

        reader_ready   = Concurrent::CountDownLatch.new(1)
        writer_finished = Concurrent::CountDownLatch.new(1)

        reader = in_thread do
          stamp = lock.try_optimistic_read
          before = data.true?           # false (old value)
          reader_ready.count_down
          writer_finished.wait(1)
          if lock.validate(stamp)
            # If the stamp validates, the value observed must still be the
            # current one (the writer could not have run yet).
            violations.increment if before != data.true?
          else
            fallback_used.increment
            rs = lock.acquire_read_lock
            begin
              expect(data.true?).to be true
            ensure
              lock.release_read_lock(rs)
            end
          end
        end

        writer = in_thread do
          reader_ready.wait(1)
          lock.with_write_lock { data.make_true }
          writer_finished.count_down
        end

        [reader, writer].each(&:join)
        expect(violations.value).to eq 0
        expect(fallback_used.value).to eq 1
      end
    end

    context '#acquire_read_lock / #release_read_lock' do

      it 'returns a stamp tagged as a read stamp' do
        stamp = subject.acquire_read_lock
        expect(stamp).to be > 0
        expect(stamp & 3).to eq StampedLock::READ_TAG
        subject.release_read_lock(stamp)
      end

      it 'allows concurrent readers' do
        s1 = subject.acquire_read_lock
        s2 = subject.acquire_read_lock
        subject.release_read_lock(s1)
        subject.release_read_lock(s2)
      end

      it 'raises IllegalOperationError when releasing a read lock not held' do
        expect { subject.release_read_lock(0) }.to raise_error(IllegalOperationError)
      end

      it 'rejects an optimistic or write stamp for release_read_lock' do
        opt = subject.try_optimistic_read
        expect { subject.release_read_lock(opt) }.to raise_error(IllegalOperationError)

        ws = subject.acquire_write_lock
        expect { subject.release_read_lock(ws) }.to raise_error(IllegalOperationError)
        subject.release_write_lock(ws)
      end

      it 'blocks while a writer is active' do
        entered = Concurrent::CountDownLatch.new(1)
        release = Concurrent::CountDownLatch.new(1)
        got_read = Concurrent::AtomicBoolean.new(false)

        writer = in_thread do
          subject.with_write_lock do
            entered.count_down
            release.wait(1)
          end
        end

        reader = in_thread do
          entered.wait(1)
          stamp = subject.acquire_read_lock
          got_read.make_true
          subject.release_read_lock(stamp)
        end

        entered.wait(1)
        sleep 0.05
        expect(got_read.true?).to be false
        release.count_down
        [writer, reader].each(&:join)
        expect(got_read.true?).to be true
      end

      it '#with_read_lock yields the stamp and releases on exceptions' do
        captured = nil
        subject.with_read_lock { |s| captured = s }
        expect(captured & 3).to eq StampedLock::READ_TAG

        expect {
          subject.with_read_lock { raise StandardError, 'boom' }
        }.to raise_error(StandardError)

        # lock is usable again
        expect { subject.with_read_lock { } }.not_to raise_error
      end

      it 'raises ArgumentError when no block is given to with_read_lock' do
        expect { subject.with_read_lock }.to raise_error(ArgumentError)
      end
    end

    context '#acquire_write_lock / #release_write_lock' do

      it 'returns a write stamp and sets write_locked?' do
        stamp = subject.acquire_write_lock
        expect(subject).to be_write_locked
        expect(stamp & 3).to eq StampedLock::WRITE_TAG
        subject.release_write_lock(stamp)
        expect(subject).not_to be_write_locked
      end

      it 'raises IllegalOperationError when releasing a write lock not held' do
        expect { subject.release_write_lock(0) }.to raise_error(IllegalOperationError)
      end

      it 'rejects an optimistic or read stamp for release_write_lock' do
        opt = subject.try_optimistic_read
        expect { subject.release_write_lock(opt) }.to raise_error(IllegalOperationError)

        rs = subject.acquire_read_lock
        expect { subject.release_write_lock(rs) }.to raise_error(IllegalOperationError)
        subject.release_read_lock(rs)
      end

      it 'raises when releasing the write lock twice' do
        stamp = subject.acquire_write_lock
        subject.release_write_lock(stamp)
        expect { subject.release_write_lock(stamp) }.to raise_error(IllegalOperationError)
      end

      it 'rejects a stale write stamp from a previous acquisition' do
        old_stamp = subject.acquire_write_lock
        subject.release_write_lock(old_stamp)

        current_stamp = subject.acquire_write_lock
        # the stale stamp must not release the currently held write lock
        expect { subject.release_write_lock(old_stamp) }.to raise_error(IllegalOperationError)
        expect(subject).to be_write_locked
        subject.release_write_lock(current_stamp)
      end

      it 'blocks while readers are active' do
        entered = Concurrent::CountDownLatch.new(1)
        release = Concurrent::CountDownLatch.new(1)
        got_write = Concurrent::AtomicBoolean.new(false)

        reader = in_thread do
          subject.with_read_lock do
            entered.count_down
            release.wait(1)
          end
        end

        writer = in_thread do
          entered.wait(1)
          stamp = subject.acquire_write_lock
          got_write.make_true
          subject.release_write_lock(stamp)
        end

        entered.wait(1)
        sleep 0.05
        expect(got_write.true?).to be false
        release.count_down
        [reader, writer].each(&:join)
        expect(got_write.true?).to be true
      end

      it '#with_write_lock yields the stamp and releases on exceptions' do
        captured = nil
        subject.with_write_lock { |s| captured = s }
        expect(captured & 3).to eq StampedLock::WRITE_TAG

        expect {
          subject.with_write_lock { raise StandardError, 'boom' }
        }.to raise_error(StandardError)
        expect(subject).not_to be_write_locked
      end

      it 'raises ArgumentError when no block is given to with_write_lock' do
        expect { subject.with_write_lock }.to raise_error(ArgumentError)
      end
    end

    context '#try_read_lock / #try_write_lock' do

      it 'try_write_lock succeeds when free and fails with readers or another writer' do
        ws = subject.try_write_lock
        expect(ws).to be > 0
        expect(subject.try_write_lock).to eq 0
        expect(subject.try_read_lock).to eq 0
        subject.release_write_lock(ws)

        rs = subject.acquire_read_lock
        expect(subject.try_write_lock).to eq 0
        subject.release_read_lock(rs)
      end

      it 'try_read_lock fails while writing' do
        ws = subject.acquire_write_lock
        expect(subject.try_read_lock).to eq 0
        subject.release_write_lock(ws)
        rs = subject.try_read_lock
        expect(rs).to be > 0
        subject.release_read_lock(rs)
      end
    end

    context '#try_convert_to_write_lock' do

      it 'returns a current write stamp unchanged' do
        ws = subject.acquire_write_lock
        expect(subject.try_convert_to_write_lock(ws)).to eq ws
        subject.release_write_lock(ws)
      end

      it 'converts a still-valid optimistic stamp' do
        stamp = subject.try_optimistic_read
        write_stamp = subject.try_convert_to_write_lock(stamp)
        expect(write_stamp).to be > 0
        expect(write_stamp & 3).to eq StampedLock::WRITE_TAG
        expect(subject).to be_write_locked
        subject.release_write_lock(write_stamp)
      end

      it 'refuses to convert a stamp invalidated by a write' do
        stamp = subject.try_optimistic_read
        ws = subject.acquire_write_lock
        subject.release_write_lock(ws)
        expect(subject.try_convert_to_write_lock(stamp)).to eq 0
      end

      it 'converts when the caller is the sole reader' do
        rs = subject.acquire_read_lock
        write_stamp = subject.try_convert_to_write_lock(rs)
        expect(write_stamp).to be > 0
        expect(subject).to be_write_locked
        # the old read hold was consumed: only the write lock remains
        subject.release_write_lock(write_stamp)
        expect(subject).not_to be_write_locked
      end

      it 'does not convert while other readers exist and preserves the read hold' do
        r1 = subject.acquire_read_lock
        r2 = subject.acquire_read_lock

        expect(subject.try_convert_to_write_lock(r1)).to eq 0

        # Original state is preserved: caller still owns its read lock, and
        # neither a write nor an additional reader was taken.
        expect(subject).not_to be_write_locked

        # Releasing both read locks succeeds with no over/under-release:
        subject.release_read_lock(r2)
        # Now the converting caller is the sole reader; conversion works.
        write_stamp = subject.try_convert_to_write_lock(r1)
        expect(write_stamp).to be > 0
        subject.release_write_lock(write_stamp)
      end

      it 'does not convert an optimistic stamp while a reader is active' do
        reader = subject.acquire_read_lock
        # a fresh optimistic stamp taken while the reader is active
        opt = subject.try_optimistic_read
        expect(opt).to be > 0
        expect(subject.try_convert_to_write_lock(opt)).to eq 0
        subject.release_read_lock(reader)
        # after the reader left, state moved, so the optimistic stamp
        # remains unconverted
        expect(subject.try_convert_to_write_lock(opt)).to eq 0
      end

      it 'refuses zero and unrelated stamps' do
        expect(subject.try_convert_to_write_lock(0)).to eq 0
        expect(subject.try_convert_to_write_lock(-5)).to eq 0
        expect(subject.try_convert_to_write_lock(nil)).to eq 0
      end
    end
  end
end
