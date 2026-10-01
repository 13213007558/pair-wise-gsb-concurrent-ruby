#!/usr/bin/env ruby

# A StampedLock supports ordinary read/write locking plus cheap optimistic
# reads: take a stamp, read without any lock, then check whether a write
# happened in the meantime. If validation fails, retry under a real read
# lock.

require 'concurrent/atomics'

class CachedPoint
  def initialize(x, y)
    @lock = Concurrent::StampedLock.new
    @x = x
    @y = y
  end

  # Writers take the exclusive write lock.
  def move(new_x, new_y)
    stamp = @lock.write_lock
    begin
      @x = new_x
      @y = new_y
    ensure
      @lock.unlock_write(stamp)
    end
  end

  # Readers first try the lock-free optimistic path. Note that passing
  # +validate+ only proves no write occurred while reading; if validation
  # fails we fall back to an ordinary read lock and read again.
  def distance_from_origin
    x, y = optimistic_read { [@x, @y] }
    Math.sqrt(x * x + y * y)
  end

  private

  def optimistic_read
    stamp = @lock.try_optimistic_read
    value = yield if stamp != 0
    return value if stamp != 0 && @lock.validate(stamp)

    # A write was in progress (or completed) during the read: take a proper
    # read lock and re-read.
    stamp = @lock.read_lock
    begin
      yield
    ensure
      @lock.unlock_read(stamp)
    end
  end
end

point = CachedPoint.new(3, 4)
puts "distance: #{point.distance_from_origin}" # => 5.0
point.move(6, 8)
puts "distance: #{point.distance_from_origin}" # => 10.0

# A valid optimistic stamp can also be upgraded to a write lock when the
# caller is the only reader, in which case the stamp is consumed:
lock = Concurrent::StampedLock.new
stamp = lock.try_optimistic_read
if write_stamp = lock.try_convert_to_write_lock(stamp)
  puts "upgraded optimistic stamp #{stamp} to write stamp #{write_stamp}"
  lock.unlock_write(write_stamp)
end
