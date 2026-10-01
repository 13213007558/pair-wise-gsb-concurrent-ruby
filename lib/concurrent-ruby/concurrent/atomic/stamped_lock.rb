require 'thread'
require 'concurrent/atomic/atomic_fixnum'
require 'concurrent/atomic/atomic_reference'
require 'concurrent/errors'
require 'concurrent/synchronization/object'

module Concurrent

  # Stamped lock which supports modes of locking.
  #
  # A StampedLock is modeled on `java.util.concurrent.locks.StampedLock`.
  # Each lock acquisition returns a stamp represented by an Integer.
  # Stamps record the state of the lock at the time they were issued and
  # are used, together with a mode, to release or convert a lock.
  #
  # The three kinds of stamps are:
  #
  # * **write** - exclusive. At most one write lock may be held, and no read
  #   lock may be held while a write lock is held.
  # * **read** - shared. Any number of read locks may be held concurrently,
  #   including re-entrantly by the same thread.
  # * **optimistic** - not a lock at all. An optimistic read stamp is a cheap
  #   snapshot token which can later be validated. Validating it only proves
  #   that no write lock has been acquired (and released, or is still held)
  #   since the stamp was issued.
  #
  # @example Typical optimistic reading pattern
  #   lock = Concurrent::StampedLock.new
  #
  #   stamp = lock.try_optimistic_read
  #   value = @value if stamp != 0
  #   unless stamp != 0 && lock.validate(stamp)
  #     stamp = lock.read_lock
  #     begin
  #       value = @value
  #     ensure
  #       lock.unlock_read(stamp)
  #     end
  #   end
  #   value
  #
  # @note Optimistic reads are *not* a replacement for a real read lock when
  #   reading data that is not itself thread safe. Passing {#validate} only
  #   establishes that the lock's mode/version has not changed; it does not
  #   make arbitrary reads of ordinary Ruby objects atomic or guarantee that
  #   a partially written object was not observed. Use a read lock (or other
  #   synchronization around the shared data) whenever that matters. The
  #   example above falls back to a read lock when validation fails.
  #
  # @note Stamps are single use values: a stamp must be unlocked exactly
  #   once, using the unlock method matching the mode in which it was
  #   acquired. Attempting to unlock a stamp of the wrong mode, a stamp that
  #   has already been unlocked, or a stamp invalidated by an intervening
  #   write raises {Concurrent::IllegalOperationError}.
  #
  # @note StampedLocks are not reentrant in write mode. Acquiring the write
  #   lock while already holding it deadlocks the calling thread, and
  #   acquiring the write lock while holding only a read lock is permitted
  #   only through {#try_convert_to_write_lock} (which fails, without
  #   releasing the read lock, while other readers are active).
  #
  # @see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/StampedLock.html
  class StampedLock < Synchronization::Object

    safe_initialization!

    # Initial state: version 1, unlocked. A non zero origin means the stamp
    # `0` is never a valid optimistic or write stamp.
    #
    # @!visibility private
    ORIGIN = 1 << 2

    # Maximum number of concurrently held read locks.
    #
    # @!visibility private
    MAX_READERS = (1 << 20) - 1

    # Width in bits of the serial field of a read stamp. Read serials are
    # reset at every write lock cycle, so this bounds only the number of read
    # grants between two writes.
    #
    # @!visibility private
    SERIAL_BITS = 32

    # Create a new StampedLock in the unlocked state.
    def initialize
      super
      # Packed state of the lock, sharing the stamp layout:
      #   * low 2 bits are the mode: OPTIMISTIC (0) while unlocked, WRITE (1)
      #     while the write lock is held
      #   * all remaining bits are the version. Acquiring the write lock sets
      #     the mode to WRITE (detected by readers validating during the
      #     write); releasing it increments the version (detected by stamps
      #     validated after the write). Taking or releasing read locks does
      #     not touch the state, so readers never invalidate stamps.
      @State       = AtomicFixnum.new(ORIGIN)
      # Stamp representing the currently held write lock. Nil when no write
      # lock is held. Reading this reference (nil vs non nil) is enough to
      # answer write stamps in #validate without taking the mutex.
      @WriteStamp  = AtomicReference.new(nil)
      @Mutex       = Mutex.new
      @Condition   = ConditionVariable.new
      @Readers     = 0
      # Number of callers waiting to acquire the write lock. While positive,
      # newly arriving readers (and non barging writers) wait, so writers are
      # not starved by a continuous stream of readers.
      @WaitingWriters = 0
      # Per-thread number of currently held read locks.
      @ReadHolds   = Hash.new(0)
      # Serial number embedded in the next issued read stamp.
      @NextSerial  = 1
      # Issued, still held read stamps: serial => owning thread.
      @ReadSerials = {}
    end

    # Returns a stamp that can later be validated, or zero if the write lock
    # is currently held. This method does not block and does not acquire any
    # lock.
    #
    # @return [Integer] a non zero optimistic read stamp if the write lock is
    #   not held, otherwise zero.
    def try_optimistic_read
      state = @State.value
      (state & MODE_MASK) == OPTIMISTIC ? state : 0
    end

    # Validates a stamp.
    #
    # An optimistic read stamp is valid if the write lock has not been
    # acquired since it was issued and is not currently held.
    #
    # A write stamp is valid if it represents the currently held write lock.
    #
    # A read stamp is valid if no write lock cycle has completed (or is in
    # progress) since it was issued.
    #
    # @param [Integer, nil] stamp the stamp to validate.
    #
    # @return [Boolean] true if the stamp is valid, false otherwise (including
    #   nil, zero, or any other value that was never a stamp).
    def validate(stamp)
      return false unless stamp.is_a?(Integer) && stamp != 0

      case stamp_kind(stamp)
      when :write
        stamp == @WriteStamp.value
      when :read
        state = @State.value
        (state & MODE_MASK) == OPTIMISTIC && read_stamp_version(stamp) == state_version(state)
      else # :optimistic
        state = @State.value
        (state & MODE_MASK) == OPTIMISTIC && state_version(state) == stamp_version(stamp)
      end
    end
    alias valid? validate

    # Non-exclusively acquires the read lock, blocking if necessary until
    # available.
    #
    # Read locks may be acquired re-entrantly by the same thread; each
    # acquisition must be matched by a separate {#unlock_read}.
    #
    # @return [Integer] the read stamp that must be passed to {#unlock_read}.
    def read_lock
      @Mutex.synchronize do
        raise ResourceLimitError, 'Too many reader threads' if @Readers >= MAX_READERS

        return grant_read_lock if available_for_read?

        begin
          @Condition.wait(@Mutex) until available_for_read?
          raise ResourceLimitError, 'Too many reader threads' if @Readers >= MAX_READERS
          grant_read_lock
        end
      end
    end

    # Non-exclusively acquires the read lock if it is available, without
    # blocking.
    #
    # @return [Integer, nil] the read stamp if acquired, otherwise nil.
    def try_read_lock
      @Mutex.synchronize do
        next nil unless available_for_read?
        next nil if @Readers >= MAX_READERS
        grant_read_lock
      end
    end

    # Releases a read lock.
    #
    # @param [Integer] stamp a read stamp returned by {#read_lock},
    #   {#try_read_lock}, or a successful conversion to a read lock.
    #
    # @return [Boolean] true.
    #
    # @raise [Concurrent::IllegalOperationError] if the stamp is not a valid,
    #   currently held read stamp: wrong mode (e.g. a write or optimistic
    #   stamp), already unlocked, or invalidated by an intervening write.
    def unlock_read(stamp)
      @Mutex.synchronize do
        serial, owner = held_read_stamp(stamp)

        @Readers -= 1
        holds = @ReadHolds[owner] - 1
        if holds <= 0
          @ReadHolds.delete(owner)
        else
          @ReadHolds[owner] = holds
        end
        @ReadSerials.delete(serial)

        @Condition.broadcast if @WaitingWriters > 0
      end
      true
    end

    # Exclusively acquires the write lock, blocking if necessary until
    # available.
    #
    # @return [Integer] the write stamp that must be passed to
    #   {#unlock_write}.
    def write_lock
      @Mutex.synchronize do
        return grant_write_lock if available_for_write?

        begin
          @WaitingWriters += 1
          @Condition.wait(@Mutex) until available_for_write?
        ensure
          @WaitingWriters -= 1
        end
        grant_write_lock
      end
    end

    # Exclusively acquires the write lock if it is available, without
    # blocking.
    #
    # @return [Integer, nil] the write stamp if acquired, otherwise nil.
    def try_write_lock
      @Mutex.synchronize do
        next nil unless available_for_write?
        grant_write_lock
      end
    end

    # Releases the write lock.
    #
    # @param [Integer] stamp a write stamp returned by {#write_lock},
    #   {#try_write_lock}, or a successful conversion to a write lock.
    #
    # @return [Boolean] true.
    #
    # @raise [Concurrent::IllegalOperationError] if the stamp does not
    #   represent the currently held write lock.
    def unlock_write(stamp)
      @Mutex.synchronize do
        unless stamp.is_a?(Integer) && stamp_kind(stamp) == :write && stamp == @WriteStamp.value
          raise IllegalOperationError,
                'attempt to unlock write lock with a stamp which is not the current write stamp'
        end

        @WriteStamp.set(nil)
        @State.update { |state| (state_version(state) + 1) << MODE_BITS }
        @Condition.broadcast
      end
      true
    end

    # If the lock state matches the given stamp, performs one of the
    # following actions:
    #
    # * The stamp represents a held write lock: it is returned.
    # * The stamp is an optimistic read stamp: returns a write stamp if the
    #   write lock is immediately available and no write has happened since
    #   the stamp was issued, otherwise nil.
    # * The stamp represents a held read lock: the write stamp is returned
    #   only if the calling thread is the sole holder of the read lock
    #   (counting re-entrant holds); otherwise nil and the read lock(s)
    #   remain held unchanged.
    #
    # Any other value (nil, an invalidated or already unlocked stamp, or a
    # stamp of an unrelated mode) results in nil without changing any lock
    # state.
    #
    # @param [Integer, nil] stamp the stamp to attempt to convert.
    #
    # @return [Integer, nil] a valid write stamp if the conversion succeeds,
    #   otherwise nil.
    def try_convert_to_write_lock(stamp)
      return nil unless stamp.is_a?(Integer) && stamp != 0

      @Mutex.synchronize do
        case stamp_kind(stamp)
        when :write
          stamp == @WriteStamp.value ? stamp : nil
        when :optimistic
          next nil unless state_version(@State.value) == stamp_version(stamp)
          next nil unless available_for_write?
          grant_write_lock
        when :read
          serial = read_stamp_serial(stamp)
          next nil unless @ReadSerials[serial] == Thread.current
          next nil unless read_stamp_version(stamp) == state_version(@State.value)
          next nil unless sole_reader?(Thread.current)

          held = @ReadHolds.delete(Thread.current)
          @Readers -= held
          @ReadSerials.delete_if { |_, owner| owner == Thread.current }
          grant_write_lock
        end
      end
    end

    # Executes a block while holding the read lock.
    #
    # @yield the task to perform while holding the read lock.
    #
    # @return [Object] the result of the block.
    #
    # @raise [ArgumentError] when no block is given.
    def with_read_lock
      raise ArgumentError, 'no block given' unless block_given?
      stamp = read_lock
      begin
        yield
      ensure
        unlock_read(stamp)
      end
    end

    # Executes a block while holding the write lock.
    #
    # @yield the task to perform while holding the write lock.
    #
    # @return [Object] the result of the block.
    #
    # @raise [ArgumentError] when no block is given.
    def with_write_lock
      raise ArgumentError, 'no block given' unless block_given?
      stamp = write_lock
      begin
        yield
      ensure
        unlock_write(stamp)
      end
    end

    # Queries whether the write lock is currently held.
    #
    # @return [Boolean] true if the write lock is held.
    def write_locked?
      (@State.value & MODE_MASK) == WRITE
    end

    private

    # @!visibility private
    #
    # Stamp layout:
    #   optimistic: [version ...........][00]
    #   write:      [version ...........][01]
    #   read:       [version ...][serial .....][10]
    MODE_BITS    = 2
    MODE_MASK    = (1 << MODE_BITS) - 1
    OPTIMISTIC   = 0b00
    WRITE        = 0b01
    READ         = 0b10
    SERIAL_MASK  = (1 << SERIAL_BITS) - 1
    SERIAL_SHIFT = MODE_BITS
    VERSION_SHIFT = MODE_BITS + SERIAL_BITS

    # @!visibility private
    def stamp_kind(stamp)
      case stamp & MODE_MASK
      when WRITE then :write
      when READ  then :read
      else            :optimistic
      end
    end

    # @!visibility private
    def encode_stamp(version, mode)
      (version << MODE_BITS) | mode
    end

    # @!visibility private
    def stamp_version(stamp)
      stamp >> MODE_BITS
    end

    # @!visibility private
    def read_stamp_serial(stamp)
      (stamp >> SERIAL_SHIFT) & SERIAL_MASK
    end

    # @!visibility private
    def read_stamp_version(stamp)
      stamp >> VERSION_SHIFT
    end

    # @!visibility private
    def make_read_stamp(version, serial)
      (version << VERSION_SHIFT) | (serial << SERIAL_SHIFT) | READ
    end

    # @!visibility private
    def state_version(state)
      state >> MODE_BITS
    end

    # @!visibility private
    def available_for_read?
      !write_locked? && @WaitingWriters == 0
    end

    # @!visibility private
    def available_for_write?
      !write_locked? && @Readers == 0
    end

    # Caller must hold @Mutex and be allowed to take the write lock.
    #
    # @!visibility private
    def grant_write_lock
      state = @State.value
      @State.update { state | WRITE }
      stamp = encode_stamp(state_version(state), WRITE)
      @WriteStamp.set(stamp)
      # All outstanding read stamps are about to be invalidated and no read
      # lock can be held, so serials can start over.
      @NextSerial = 1
      stamp
    end

    # Caller must hold @Mutex and be allowed to take a read lock.
    #
    # @!visibility private
    def grant_read_lock
      @Readers += 1
      @ReadHolds[Thread.current] += 1
      serial = @NextSerial
      if serial > SERIAL_MASK
        raise ResourceLimitError, 'Too many read lock grants between writes'
      end
      @NextSerial += 1
      @ReadSerials[serial] = Thread.current
      make_read_stamp(state_version(@State.value), serial)
    end

    # Verifies that +stamp+ is a currently held read stamp and returns its
    # serial together with the thread that acquired it.
    #
    # @!visibility private
    def held_read_stamp(stamp)
      unless stamp.is_a?(Integer) && stamp_kind(stamp) == :read
        raise IllegalOperationError,
              'attempt to release a read lock with a non-read stamp'
      end

      serial = read_stamp_serial(stamp)
      owner  = @ReadSerials[serial]

      if owner.nil?
        raise IllegalOperationError,
              'attempt to release a read lock with an invalid or already unlocked stamp'
      elsif read_stamp_version(stamp) != state_version(@State.value)
        raise IllegalOperationError,
              'attempt to release a read lock whose stamp was invalidated by a write'
      end

      [serial, owner]
    end

    # @!visibility private
    def sole_reader?(thread)
      @Readers == @ReadHolds[thread]
    end
  end
end
