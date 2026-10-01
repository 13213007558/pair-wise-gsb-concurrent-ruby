#!/usr/bin/env ruby

# Example: optimistic reads with Concurrent::StampedLock
#
# Run with:
#   ruby -Ilib/concurrent-ruby examples/stamped_lock_example.rb

require 'concurrent/atomic/stamped_lock'
require 'concurrent/atomic/count_down_latch'

lock = Concurrent::StampedLock.new
data = { count: 0 }

# The recommended read pattern: take an optimistic stamp, read, then
# validate. If a write raced with us, fall back to a pessimistic read lock
# and read again.
def read_value(lock, data)
  stamp = lock.try_optimistic_read
  value = data[:count]

  return [value, :optimistic] if lock.validate(stamp)

  stamp = lock.acquire_read_lock
  begin
    [data[:count], :read_lock]
  ensure
    lock.release_read_lock(stamp)
  end
end

def write_value(lock, data, new_value)
  stamp = lock.acquire_write_lock
  begin
    data[:count] = new_value
  ensure
    lock.release_write_lock(stamp)
  end
end

# 1) Uncontended read: the optimistic stamp validates, no lock is taken.
value, mode = read_value(lock, data)
puts "uncontended read: value=#{value} mode=#{mode}"

# 2) Deterministic interleaving: a full write cycle happens between taking
#    the optimistic stamp and validating it. The stale read is detected.
stamp = lock.try_optimistic_read
stale = data[:count]
write_value(lock, data, 41)
puts "read #{stale} under old stamp, valid after write? #{lock.validate(stamp)}"

value, = read_value(lock, data)
puts "re-read after failed validation sees: #{value}"

# 3) A writer is in flight: try_optimistic_read returns 0 and the reader
#    blocks on the read lock until the writer is done.
held    = Concurrent::CountDownLatch.new(1)
release = Concurrent::CountDownLatch.new(1)

writer = Thread.new do
  lock.with_write_lock do
    held.count_down
    release.wait(2)
    data[:count] = 99
  end
end

held.wait(2)
value, mode = read_value(lock, data) # blocks here until the writer releases
puts "read while writer was active: value=#{value} mode=#{mode}"
release.count_down
writer.join

# Note: the stamp only certifies that no write crossed this lock. Reading
# ordinary Ruby state without a lock is not automatically thread-safe; the
# fallback above is what makes the read correct when validation fails.
