require 'concurrent/atomic/atomic_fixnum'
require 'concurrent/errors'
require 'concurrent/synchronization/object'
require 'concurrent/synchronization/lock'

module Concurrent

  # Stamped lock implementation providing optimistic (lock-free) reads
  # together with an exclusive write lock and a shared (non-reentrant)
  # read lock.
  #
  # The API is inspired by `java.util.concurrent.locks.StampedLock`. All
  # lock acquisitions return an integer *stamp* which must later be passed
  # back to the matching release method. A return value of zero always
  # signals failure to acquire a lock or to convert a stamp.
  #
  # == Optimistic reads
  #
  # {#try_optimistic_read} returns a stamp *without* taking any lock and
  # *without* blocking writers. After reading the protected state, call
  # {#validate} to find out whether a write lock was held, or completed a
  # full acquire/release cycle, in the meantime. If validation fails the
  # values just read may be stale and must be re-read, usually under a
  # pessimistic read lock:
  #
  # @example optimistic read with pessimistic fallback
  #   stamp = lock.try_optimistic_read
  #   x, y = pos.x, pos.y
  #   unless lock.validate(stamp)
  #     stamp = lock.acquire_read_lock
  #     begin
  #       x, y = pos.x, pos.y
  #     ensure
  #       lock.release_read_lock(stamp)
  #     end
  #   end
  #
  # @note An optimistic stamp only certifies that *this lock* observed no
  #   write between {#try_optimistic_read} and {#validate}. Reading an
  #   ordinary Ruby object without a lock is **not** made thread-safe by
  #   this or any other primitive: the interpreter may interleave the
  #   individual attribute reads, and a value can be torn or inconsistent
  #   with other values read in the same optimistic section. Optimistic
  #   reads are therefore only sound when a failed validation is handled
  #   (typically by re-reading under {#acquire_read_lock}), and only for
  #   state whose writers are correctly guarded by the write lock.
  #
  # @note The read lock is **not reentrant** and stamps do not record the
  #   owning thread. Do not acquire a read lock twice from the same thread
  #   without releasing it, and always release a stamp through the method
  #   matching its mode. Releasing a read stamp as a write stamp (or vice
  #   versa) raises {Concurrent::IllegalOperationError}.
  #
  # @see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/StampedLock.html
  #   java.util.concurrent.locks.StampedLock
  class StampedLock < Synchronization::Object

    # @!visibility private
    # The single state word is laid out as:
    #
    #   bit  0 .. 29  : number of read locks currently held
    #   bit 30        : set while the write lock is held
    #   bit 31 ..     : monotonically bumped version
    #
    # Acquiring the write lock adds WBIT (sets the write bit, readers are
    # zero at that point); releasing it adds WBIT again, which clears the
    # bit and carries into the version field. Consequently a completed
    # write cycle always changes the state word and invalidates every
    # optimistic stamp taken before it.
    WBIT = 1 << 30

    # @!visibility private
    SBITS = WBIT - 1

    # @!visibility private
    MAX_READERS = SBITS

    # Tags stored in the low two bits of every stamp handed out:
    # @!visibility private
    OPT_TAG   = 1 # optimistic read stamp
    # @!visibility private
    READ_TAG  = 2 # pessimistic read lock stamp
    # @!visibility private
    WRITE_TAG = 3 # write lock stamp

    safe_initialization!

    # Create a new `StampedLock` in the unlocked state.
    def initialize
      super()
      @State = AtomicFixnum.new(0)
      @Mutex = Synchronization::Lock.new
    end

    # Return a stamp that can later be validated, or zero if the write
    # lock is currently held.
    #
    # This method does not take a lock and does not block writers; it is
    # safe to call repeatedly.
    #
    # @return [Integer] a non-zero optimistic stamp, or zero if the write
    #   lock is currently held
    def try_optimistic_read
      state = @State.value
      (state & WBIT) == 0 ? ((state << 2) | OPT_TAG) : 0
    end

    # Validate an optimistic read stamp.
    #
    # @param [Integer] stamp a stamp previously returned by
    #   {#try_optimistic_read}
    #
    # @return [Boolean] true if the write lock has not been acquired since
    #   the stamp was handed out (and no read lock acquire/release moved
    #   the state); false otherwise. Returns false for zero or any stamp
    #   that is not an optimistic stamp.
    def validate(stamp)
      return false unless stamp.is_a?(::Integer) && stamp > 0
      return false unless (stamp & 3) == OPT_TAG
      @State.value == (stamp >> 2)
    end

    # Acquire the read lock, blocking until it is available and the write
    # lock is not held.
    #
    # @return [Integer] a non-zero read stamp that must be passed to
    #   {#release_read_lock}
    #
    # @raise [Concurrent::ResourceLimitError] if the maximum number of
    #   concurrent readers is exceeded
    def acquire_read_lock
      @Mutex.synchronize do
        @Mutex.ns_wait until (@State.value & WBIT) == 0
        readers = @State.value & SBITS
        raise ResourceLimitError.new('Too many reader threads') if readers >= MAX_READERS
        state = @State.value + 1
        @State.value = state
        (state << 2) | READ_TAG
      end
    end

    # Try to acquire the read lock without blocking.
    #
    # @return [Integer] a non-zero read stamp, or zero if the write lock
    #   is currently held
    #
    # @raise [Concurrent::ResourceLimitError] if the maximum number of
    #   concurrent readers is exceeded
    def try_read_lock
      @Mutex.synchronize do
        if (@State.value & WBIT) == 0
          readers = @State.value & SBITS
          raise ResourceLimitError.new('Too many reader threads') if readers >= MAX_READERS
          state = @State.value + 1
          @State.value = state
          (state << 2) | READ_TAG
        else
          0
        end
      end
    end

    # Release a read lock previously acquired with {#acquire_read_lock} or
    # {#try_read_lock}.
    #
    # @param [Integer] stamp the read stamp returned at acquisition
    #
    # @return [Boolean] true if the lock was successfully released
    #
    # @raise [Concurrent::IllegalOperationError] if `stamp` is not a read
    #   stamp, or if no read lock is currently held
    def release_read_lock(stamp)
      unless stamp.is_a?(::Integer) && stamp > 0 && (stamp & 3) == READ_TAG
        raise IllegalOperationError, 'Cannot release a read lock with a stamp that is not a read stamp'
      end

      @Mutex.synchronize do
        state = @State.value
        if (state & SBITS) == 0
          raise IllegalOperationError, 'Cannot release a read lock which is not held'
        end
        @State.value = state - 1
        @Mutex.ns_broadcast
      end
      true
    end

    # Acquire the write lock, blocking until it is available, i.e. until
    # neither the write lock nor any read lock is held.
    #
    # @return [Integer] a non-zero write stamp that must be passed to
    #   {#release_write_lock}
    def acquire_write_lock
      @Mutex.synchronize do
        @Mutex.ns_wait until (@State.value & (WBIT | SBITS)).zero?
        state = @State.value + WBIT
        @State.value = state
        (state << 2) | WRITE_TAG
      end
    end

    # Try to acquire the write lock without blocking.
    #
    # @return [Integer] a non-zero write stamp, or zero if either the read
    #   or write lock is currently held
    def try_write_lock
      @Mutex.synchronize do
        if (state = @State.value) & (WBIT | SBITS) == 0
          state += WBIT
          @State.value = state
          (state << 2) | WRITE_TAG
        else
          0
        end
      end
    end

    # Release the write lock.
    #
    # @param [Integer] stamp the write stamp returned at acquisition
    #
    # @return [Boolean] true if the lock was successfully released
    #
    # @raise [Concurrent::IllegalOperationError] if `stamp` is not a write
    #   stamp, if the write lock is not currently held, or if it is a stale
    #   write stamp from a previous (already released) acquisition
    def release_write_lock(stamp)
      unless stamp.is_a?(::Integer) && stamp > 0 && (stamp & 3) == WRITE_TAG
        raise IllegalOperationError, 'Cannot release a write lock with a stamp that is not a write stamp'
      end

      @Mutex.synchronize do
        unless (stamp >> 2) == @State.value && (@State.value & WBIT) != 0
          raise IllegalOperationError, 'Cannot release a write lock which is not held by this stamp'
        end
        @State.value = @State.value + WBIT # clears the bit and bumps the version
        @Mutex.ns_broadcast
      end
      true
    end

    # Try to convert a valid stamp into a write stamp without blocking.
    #
    # The conversion, when successful, consumes the mode represented by
    # `stamp` (an optimistic stamp consumes nothing) and returns a fresh
    # write stamp. On failure zero is returned and the caller keeps
    # exactly the holds it had before the call:
    #
    # * a current **write stamp** is returned unchanged;
    # * an **optimistic stamp** converts only if it still validates and no
    #   read or write lock is held;
    # * a **read stamp** converts only if it is the sole outstanding read
    #   lock and no write lock is held; otherwise the read lock remains
    #   held by the caller.
    #
    # @param [Integer] stamp an optimistic, read or write stamp
    #
    # @return [Integer] a non-zero write stamp on success, zero on failure
    def try_convert_to_write_lock(stamp)
      return 0 unless stamp.is_a?(::Integer) && stamp > 0

      case stamp & 3
      when WRITE_TAG
        @Mutex.synchronize do
          return stamp if (stamp >> 2) == @State.value && (@State.value & WBIT) != 0
        end
      when OPT_TAG
        @Mutex.synchronize do
          state = @State.value
          if (stamp >> 2) == state && state & (WBIT | SBITS) == 0
            state += WBIT
            @State.value = state
            return (state << 2) | WRITE_TAG
          end
        end
      when READ_TAG
        @Mutex.synchronize do
          state = @State.value
          if (stamp >> 2) == state && (state & SBITS) == 1
            # The single outstanding reader is this stamp: turn its hold
            # into the write lock without ever leaving the lock free.
            state = state - 1 + WBIT
            @State.value = state
            return (state << 2) | WRITE_TAG
          end
        end
      end

      0
    end

    # Execute a block while holding the read lock.
    #
    # @yield [Integer] stamp the read stamp held for the duration of the block
    #
    # @return [Object] the result of the block
    #
    # @raise [ArgumentError] when no block is given
    # @raise [Concurrent::ResourceLimitError] if the maximum number of
    #   concurrent readers is exceeded
    def with_read_lock
      raise ArgumentError.new('no block given') unless block_given?
      stamp = acquire_read_lock
      begin
        yield stamp
      ensure
        release_read_lock(stamp)
      end
    end

    # Execute a block while holding the write lock.
    #
    # @yield [Integer] stamp the write stamp held for the duration of the block
    #
    # @return [Object] the result of the block
    #
    # @raise [ArgumentError] when no block is given
    def with_write_lock
      raise ArgumentError.new('no block given') unless block_given?
      stamp = acquire_write_lock
      begin
        yield stamp
      ensure
        release_write_lock(stamp)
      end
    end

    # Query whether the write lock is currently held by any thread.
    #
    # @return [Boolean] true if the write lock is held
    def write_locked?
      (@State.value & WBIT) != 0
    end
  end
end
