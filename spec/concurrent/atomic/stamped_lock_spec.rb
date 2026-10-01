require 'concurrent/atomic/stamped_lock'
require 'concurrent/atomic/count_down_latch'
require 'concurrent/atomic/atomic_boolean'
require 'concurrent/atomic/atomic_fixnum'

module Concurrent

  RSpec.describe StampedLock do

    def new_latch(count = 1)
      Concurrent::CountDownLatch.new(count)
    end

    # A one-shot thread rendezvous built from two queues (nil objects).
    class Rendezvous
      def initialize
        @go = Queue.new
        @done = Queue.new
      end

      # Block until #go! is called.
      def wait
        @go.pop
      end

      # Release a thread blocked in #wait.
      def go!
        @go << true
      end

      # Mark this step finished; unblocks a thread in #join.
      def finish!
        @done << true
      end

      # Block until #finish! is called.
      def join
        @done.pop
      end
    end

    # Executes a fully sequenced interleaving of a reader thread and a
    # writer/other thread:
    #   1. the reader performs +reader_first+
    #   2. the other thread performs +other+
    #   3. the reader performs +reader_last+
    # Each step completes before the next starts, so the interleaving cannot
    # depend on thread scheduling.
    def sequenced_interleaving(reader_first, other, reader_last)
      step1 = Rendezvous.new
      step1_done = Rendezvous.new
      step3 = Rendezvous.new

      reader = in_thread do
        step1.wait
        reader_first.call
        step1_done.finish!

        step3.wait
        reader_last.call
      end

      step1.go!
      step1_done.join

      other.call

      step3.go!
      reader.join
    end

    context '#try_optimistic_read' do

      it 'returns a non zero stamp when the write lock is not held' do
        expect(subject.try_optimistic_read).to_not eq 0
      end

      it 'returns zero while the write lock is held' do
        started = new_latch
        release = new_latch

        in_thread do
          @ws = subject.write_lock
          started.count_down
          release.wait(1)
          subject.unlock_write(@ws)
        end

        started.wait(1)
        expect(subject.write_locked?).to be true
        expect(subject.try_optimistic_read).to eq 0
        release.count_down
      end

      it 'does not block other readers and is not itself a held lock' do
        stamp = subject.with_read_lock { subject.try_optimistic_read }
        expect(stamp).to_not eq 0
        expect(subject.validate(stamp)).to be true
      end
    end

    context '#validate' do

      it 'returns true for a fresh optimistic stamp with no intervening write' do
        stamp = subject.try_optimistic_read
        expect(subject.validate(stamp)).to be true
      end

      it 'returns true for an optimistic stamp when only read locks are taken' do
        stamp = subject.try_optimistic_read
        rs = subject.read_lock
        subject.unlock_read(rs)
        expect(subject.validate(stamp)).to be true
      end

      it 'returns false for nil, zero and values which are not stamps' do
        expect(subject.validate(nil)).to be false
        expect(subject.validate(0)).to be false
        expect(subject.validate('not a stamp')).to be false
        expect(subject.validate(Object.new)).to be false
      end

      it 'returns false for an optimistic stamp invalidated by a completed write' do
        stamp = subject.try_optimistic_read
        subject.with_write_lock { }
        expect(subject.validate(stamp)).to be false
      end

      it 'returns false for an optimistic stamp while a write is in progress' do
        started = new_latch
        release = new_latch
        stamp = subject.try_optimistic_read

        in_thread do
          @ws = subject.write_lock
          started.count_down
          release.wait(1)
          subject.unlock_write(@ws)
        end

        started.wait(1)
        expect(subject.write_locked?).to be true
        expect(subject.validate(stamp)).to be false
        release.count_down
      end

      it 'returns true for the current write stamp and false for a stale one' do
        ws = subject.write_lock
        expect(subject.validate(ws)).to be true
        subject.unlock_write(ws)
        expect(subject.validate(ws)).to be false

        ws2 = subject.write_lock
        expect(subject.validate(ws)).to be false
        expect(subject.validate(ws2)).to be true
        subject.unlock_write(ws2)
        expect(subject.validate(ws2)).to be false
      end

      it 'returns true for a read stamp until a write occurs' do
        # A read stamp encodes the version, not the hold count, so (like the
        # JDK primitive) it remains valid after being unlocked as long as no
        # write has happened; it is invalidated by the next write cycle.
        rs = subject.read_lock
        expect(subject.validate(rs)).to be true
        subject.unlock_read(rs)
        expect(subject.validate(rs)).to be true
        subject.with_write_lock { }
        expect(subject.validate(rs)).to be false
      end

      it 'returns false for a read stamp while a write is in progress' do
        rs = subject.read_lock
        subject.unlock_read(rs)

        writer_started = new_latch
        writer_release = new_latch
        writer = in_thread do
          @ws = subject.write_lock
          writer_started.count_down
          writer_release.wait(1)
          subject.unlock_write(@ws)
        end

        writer_started.wait(1)
        # writer now holds the write lock; the old read stamp must be invalid
        expect(subject.write_locked?).to be true
        expect(subject.validate(rs)).to be false
        writer_release.count_down
        writer.join
        expect(subject.validate(rs)).to be false
      end
    end

    context 'deterministic optimistic read interleavings' do

      # This is the key property: it must be impossible to both observe an
      # old value and have validate return true. The interleavings below are
      # fully sequenced with latches, so they fail deterministically against
      # an implementation that bumps the version on the wrong side of the
      # write or forgets to invalidate while a write is held.

      it 'invalidates an optimistic stamp when the write finishes between read and validate' do
        value = Concurrent::AtomicReference.new(:old)
        reader_saw_old = nil
        reader_validated = nil
        stamp = nil

        reader_first = lambda do
          stamp = subject.try_optimistic_read
          reader_saw_old = (value.value == :old)
        end

        other = lambda do
          # write completes entirely between snapshot and validation
          subject.with_write_lock { value.set(:new) }
        end

        reader_last = lambda do
          reader_validated = subject.validate(stamp)
        end

        sequenced_interleaving(reader_first, other, reader_last)

        expect(reader_saw_old).to be true
        expect(reader_validated).to be false
      end

      it 'invalidates an optimistic stamp when a write is held during validate' do
        value = Concurrent::AtomicReference.new(:old)
        reader_saw_old = nil
        reader_validated = nil
        stamp = nil
        write_acquired = Rendezvous.new
        release_write = Rendezvous.new

        reader_first = lambda do
          stamp = subject.try_optimistic_read
          reader_saw_old = (value.value == :old)
        end

        other = lambda do
          in_thread do
            @ws = subject.write_lock
            value.set(:new)
            write_acquired.finish!
            release_write.wait
            subject.unlock_write(@ws)
          end
          write_acquired.join
          # writer holds the write lock while the reader validates
          @_release_write = release_write
        end

        reader_last = lambda do
          reader_validated = subject.validate(stamp)
        end

        sequenced_interleaving(reader_first, other, reader_last)
        @_release_write.go!

        expect(reader_saw_old).to be true
        expect(reader_validated).to be false
      end

      it 'keeps an optimistic stamp valid when no write happens across the snapshot' do
        value = Concurrent::AtomicReference.new(:only_ever_value)
        snapshot = nil
        validated = nil
        stamp = nil

        reader_first = lambda do
          stamp = subject.try_optimistic_read
          snapshot = value.value
        end

        other = lambda do
          # a reader, not a writer, runs in between
          subject.with_read_lock { value.value }
        end

        reader_last = lambda do
          validated = subject.validate(stamp)
        end

        sequenced_interleaving(reader_first, other, reader_last)

        expect(snapshot).to eq :only_ever_value
        expect(validated).to be true
      end
    end

    context '#read_lock / #unlock_read' do

      it 'returns stamps which validate while held' do
        stamp = subject.read_lock
        expect(stamp).to be_an(Integer)
        expect(subject.validate(stamp)).to be true
        expect(subject.unlock_read(stamp)).to be true
        expect(subject.validate(stamp)).to be true # version unchanged by unlock
        subject.with_write_lock { }
        expect(subject.validate(stamp)).to be false
      end

      it 'allows concurrent readers' do
        a = subject.read_lock
        b = subject.read_lock
        expect(subject.validate(a)).to be true
        expect(subject.validate(b)).to be true
        subject.unlock_read(a)
        subject.unlock_read(b)
      end

      it 'is re-entrant for the same thread and requires a matching number of unlocks' do
        a = subject.read_lock
        b = subject.read_lock
        subject.unlock_read(a)
        expect(subject.validate(b)).to be true
        subject.unlock_read(b)
      end

      it 'blocks while a write lock is held' do
        started = new_latch
        release = new_latch
        read_acquired = Concurrent::AtomicBoolean.new(false)

        in_thread do
          @ws = subject.write_lock
          started.count_down
          release.wait(1)
          subject.unlock_write(@ws)
        end

        reader = in_thread do
          started.wait(1)
          @rs = subject.read_lock
          read_acquired.make_true
          subject.unlock_read(@rs)
        end

        started.wait(1)
        sleep 0.05
        expect(read_acquired.true?).to be false
        release.count_down
        reader.join(1)
        expect(read_acquired.true?).to be true
      end

      it 'waits behind a queued writer so writers are not starved' do
        # reader holds the lock; a writer queues; a second reader must not
        # barge in ahead of the writer
        r1 = subject.read_lock
        writer_started = new_latch
        writer_done = new_latch
        writer_saw_readers = Concurrent::AtomicBoolean.new(false)

        writer = in_thread do
          writer_started.count_down
          @ws = subject.write_lock
          writer_saw_readers.make_true
          writer_done.count_down
          subject.unlock_write(@ws)
        end

        writer_started.wait(1)
        sleep 0.05 # let the writer block in #write_lock

        second_reader_acquired = Concurrent::AtomicBoolean.new(false)
        second_reader = in_thread do
          @r2 = subject.read_lock
          second_reader_acquired.make_true
          subject.unlock_read(@r2)
        end

        sleep 0.05
        expect(second_reader_acquired.true?).to be false
        subject.unlock_read(r1)
        writer_done.wait(1)
        expect(writer_saw_readers.true?).to be true
        second_reader.join(1)
        expect(second_reader_acquired.true?).to be true
      end
    end

    context '#try_read_lock' do

      it 'acquires when free' do
        stamp = subject.try_read_lock
        expect(stamp).to be_an(Integer)
        subject.unlock_read(stamp)
      end

      it 'returns nil while a writer holds the lock' do
        ws = subject.write_lock
        expect(subject.try_read_lock).to be_nil
        subject.unlock_write(ws)
      end

      it 'returns nil while a writer is queued' do
        r = subject.read_lock
        queued = new_latch
        writer = in_thread do
          queued.count_down
          @ws = subject.write_lock
          subject.unlock_write(@ws)
        end
        queued.wait(1)
        sleep 0.05
        expect(subject.try_read_lock).to be_nil
        subject.unlock_read(r)
        writer.join
      end
    end

    context '#write_lock / #unlock_write' do

      it 'returns a valid write stamp and blocks readers while held' do
        stamp = subject.write_lock
        expect(subject.validate(stamp)).to be true
        expect(subject.write_locked?).to be true
        expect(subject.try_read_lock).to be_nil
        expect(subject.try_optimistic_read).to eq 0
        subject.unlock_write(stamp)
        expect(subject.write_locked?).to be false
      end

      it 'blocks behind an active reader' do
        rs = subject.read_lock
        acquired = Concurrent::AtomicBoolean.new(false)
        writer = in_thread do
          @ws = subject.write_lock
          acquired.make_true
          subject.unlock_write(@ws)
        end
        sleep 0.05
        expect(acquired.true?).to be false
        subject.unlock_read(rs)
        writer.join(1)
        expect(acquired.true?).to be true
      end

      it 'serializes writers and bumps the version for every cycle' do
        first = subject.try_optimistic_read
        3.times do
          ws = subject.write_lock
          subject.unlock_write(ws)
        end
        expect(subject.validate(first)).to be false
        fresh = subject.try_optimistic_read
        expect(subject.validate(fresh)).to be true
      end
    end

    context '#try_write_lock' do

      it 'acquires when free' do
        stamp = subject.try_write_lock
        expect(stamp).to be_an(Integer)
        subject.unlock_write(stamp)
      end

      it 'returns nil when a reader or writer holds the lock' do
        rs = subject.read_lock
        expect(subject.try_write_lock).to be_nil
        subject.unlock_read(rs)

        ws = subject.write_lock
        expect(subject.try_write_lock).to be_nil
        subject.unlock_write(ws)
      end
    end

    context 'wrong mode unlocks are rejected' do

      it 'rejects unlocking a write lock with an optimistic stamp' do
        opt = subject.try_optimistic_read
        subject.write_lock
        expect { subject.unlock_write(opt) }.to raise_error(IllegalOperationError)
        subject.unlock_write(subject.instance_variable_get(:@WriteStamp).value)
      end

      it 'rejects unlocking a write lock with a read stamp, nil, or garbage' do
        subject.write_lock
        expect { subject.unlock_write(nil) }.to raise_error(IllegalOperationError)
        expect { subject.unlock_write(123456) }.to raise_error(IllegalOperationError)
        subject.unlock_write(subject.instance_variable_get(:@WriteStamp).value)

        rs = subject.read_lock
        expect { subject.unlock_write(rs) }.to raise_error(IllegalOperationError)
        subject.unlock_read(rs)
      end

      it 'rejects unlocking a read lock with an optimistic or write stamp, nil, or garbage' do
        subject.read_lock
        expect { subject.unlock_read(subject.try_optimistic_read) }
          .to raise_error(IllegalOperationError)
        expect { subject.unlock_read(nil) }.to raise_error(IllegalOperationError)
        expect { subject.unlock_read(0) }.to raise_error(IllegalOperationError)
        expect { subject.unlock_read(Object.new) }.to raise_error(IllegalOperationError)
      end

      it 'rejects unlocking a read lock with a write stamp' do
        rs = subject.read_lock
        subject.unlock_read(rs)
        ws = subject.write_lock
        expect { subject.unlock_read(ws) }.to raise_error(IllegalOperationError)
        subject.unlock_write(ws)
      end

      it 'rejects double unlock of the same read stamp' do
        stamp = subject.read_lock
        subject.unlock_read(stamp)
        expect { subject.unlock_read(stamp) }.to raise_error(IllegalOperationError)
      end

      it 'rejects double unlock of the same write stamp' do
        stamp = subject.write_lock
        subject.unlock_write(stamp)
        expect { subject.unlock_write(stamp) }.to raise_error(IllegalOperationError)
      end

      it 'rejects unlocking a read stamp invalidated by a write' do
        stamp = subject.read_lock
        # hand the stamp to another thread: release, run a full write cycle,
        # then attempt to unlock with the stale stamp token
        token = stamp
        subject.unlock_read(token)
        subject.with_write_lock { }
        expect { subject.unlock_read(token) }.to raise_error(IllegalOperationError)
      end
    end

    context '#try_convert_to_write_lock' do

      it 'returns the stamp unchanged when already holding the write lock' do
        ws = subject.write_lock
        expect(subject.try_convert_to_write_lock(ws)).to eq ws
        subject.unlock_write(ws)
      end

      it 'converts a valid optimistic stamp when the write lock is available' do
        opt = subject.try_optimistic_read
        ws = subject.try_convert_to_write_lock(opt)
        expect(ws).to be_an(Integer)
        expect(subject.write_locked?).to be true
        expect(subject.validate(opt)).to be false
        subject.unlock_write(ws)
      end

      it 'does not convert an optimistic stamp when readers are active' do
        opt = subject.try_optimistic_read
        held = new_latch
        release = new_latch
        reader = in_thread do
          @rs = subject.read_lock
          held.count_down
          release.wait(1)
          subject.unlock_read(@rs)
        end
        held.wait(1)
        expect(subject.try_convert_to_write_lock(opt)).to be_nil
        expect(subject.write_locked?).to be false
        release.count_down
        reader.join
      end

      it 'does not convert an optimistic stamp stale after a write' do
        opt = subject.try_optimistic_read
        subject.with_write_lock { }
        expect(subject.try_convert_to_write_lock(opt)).to be_nil
      end

      it 'converts the sole read stamp into a write stamp' do
        rs = subject.read_lock
        ws = subject.try_convert_to_write_lock(rs)
        expect(ws).to be_an(Integer)
        expect(subject.write_locked?).to be true
        # the read stamp is gone: unlocking it as a read fails
        expect { subject.unlock_read(rs) }.to raise_error(IllegalOperationError)
        subject.unlock_write(ws)
      end

      it 'converts all re-entrant read holds of the sole reader' do
        a = subject.read_lock
        b = subject.read_lock
        ws = subject.try_convert_to_write_lock(a)
        expect(ws).to be_an(Integer)
        expect(subject.write_locked?).to be true
        expect { subject.unlock_read(b) }.to raise_error(IllegalOperationError)
        subject.unlock_write(ws)
        expect(subject.write_locked?).to be false
      end

      it 'fails to convert when another reader is active and leaves the read lock held' do
        rs = subject.read_lock

        held = new_latch
        release = new_latch
        other = in_thread do
          @other = subject.read_lock
          held.count_down
          release.wait(1)
          subject.unlock_read(@other)
        end
        held.wait(1)

        expect(subject.try_convert_to_write_lock(rs)).to be_nil
        # original read hold must be fully intact: still valid, still blocks
        # writers, and still needs exactly one unlock
        expect(subject.validate(rs)).to be true
        expect(subject.try_write_lock).to be_nil
        expect(subject.write_locked?).to be false

        release.count_down
        other.join
        expect(subject.try_write_lock).to be_nil # our read is still held
        subject.unlock_read(rs)
        ws = subject.try_write_lock
        expect(ws).to be_an(Integer)
        subject.unlock_write(ws)
      end

      it 'returns nil without state change for invalid inputs' do
        expect(subject.try_convert_to_write_lock(nil)).to be_nil
        expect(subject.try_convert_to_write_lock(0)).to be_nil
        expect(subject.try_convert_to_write_lock('x')).to be_nil
        expect(subject.write_locked?).to be false

        rs = subject.read_lock
        subject.unlock_read(rs)
        expect(subject.try_convert_to_write_lock(rs)).to be_nil
      end
    end

    context '#with_read_lock and #with_write_lock' do

      it 'runs the block under a read lock and releases it' do
        entered = false
        result = subject.with_read_lock do
          entered = true
          second = subject.try_read_lock
          expect(second).to be_an(Integer)
          subject.unlock_read(second)
          :answer
        end
        expect(entered).to be true
        expect(result).to eq :answer
        ws = subject.try_write_lock
        expect(ws).to be_an(Integer)
        subject.unlock_write(ws)
      end

      it 'runs the block under a write lock and releases it' do
        result = subject.with_write_lock do
          expect(subject.write_locked?).to be true
          42
        end
        expect(result).to eq 42
        expect(subject.write_locked?).to be false
      end

      it 'releases the lock when the block raises' do
        expect { subject.with_write_lock { raise 'boom' } }.to raise_error('boom')
        expect(subject.write_locked?).to be false
        ws = subject.try_write_lock
        expect(ws).to be_an(Integer)
        subject.unlock_write(ws)
      end

      it 'requires a block' do
        expect { subject.with_read_lock }.to raise_error(ArgumentError)
        expect { subject.with_write_lock }.to raise_error(ArgumentError)
      end
    end

    context 'contention', :stress do

      it 'keeps readers/writers mutually consistent under heavy concurrency' do
        value = Concurrent::AtomicFixnum.new(0)
        threads = []

        4.times do
          threads << in_thread do
            50.times do
              subject.with_write_lock { value.increment }
            end
          end
        end

        8.times do
          threads << in_thread do
            100.times do
              stamp = subject.read_lock
              begin
                v1 = value.value
                v2 = value.value
                expect(v1).to eq(v2)
              ensure
                subject.unlock_read(stamp)
              end
            end
          end
        end

        threads.each(&:join)
        expect(value.value).to eq(4 * 50)
      end

      it 'never reports a valid optimistic stamp together with a torn write' do
        # writers publish a matched pair under the write lock; an optimistic
        # snapshot that validates must see a matched pair (the pair is read
        # via atomics so the reads themselves are coherent)
        left = Concurrent::AtomicFixnum.new(0)
        right = Concurrent::AtomicFixnum.new(0)
        mismatches = Concurrent::AtomicFixnum.new(0)
        threads = []

        2.times do |i|
          threads << in_thread do
            100.times do |j|
              n = i * 1000 + j + 1
              subject.with_write_lock do
                left.value = n
                right.value = n
              end
            end
          end
        end

        4.times do
          threads << in_thread do
            200.times do
              stamp = subject.try_optimistic_read
              l = left.value
              r = right.value
              if stamp != 0 && subject.validate(stamp)
                mismatches.increment if l != r
              end
            end
          end
        end

        threads.each(&:join)
        expect(mismatches.value).to eq 0
      end
    end
  end
end
