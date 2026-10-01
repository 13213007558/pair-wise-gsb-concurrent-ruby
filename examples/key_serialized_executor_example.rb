#!/usr/bin/env ruby

# Demonstrates Concurrent::KeySerializedExecutor.
#
# A single fixed thread pool with TWO worker threads is shared by every key.
# The program proves that:
#
#   1. Many tasks posted in a row for one key never overlap each other and run
#      strictly in submission order.
#   2. While that key keeps submitting work, a different key still makes
#      progress (it runs on the other worker) - a busy key cannot starve the
#      rest of the pool.
#
# Run:
#   ruby -I lib/concurrent-ruby examples/key_serialized_executor_example.rb

require 'concurrent'

executor = Concurrent::KeySerializedExecutor.new(2, max_queue: 1000)

# Shared bookkeeping ---------------------------------------------------------

completion_order = []       # [key, index] pairs, in completion order
order_mutex      = Mutex.new
in_flight        = Hash.new(0)
overlap_violations = Concurrent::AtomicFixnum.new(0)
finished         = Concurrent::CountDownLatch.new(24)

run_task = lambda do |key, index|
  order_mutex.synchronize do
    # Same key must never be running on two workers at once.
    overlap_violations.increment if in_flight[key].positive?
    in_flight[key] += 1
  end

  sleep 0.005 # simulate work; key A keeps a worker almost constantly busy

  order_mutex.synchronize do
    in_flight[key] -= 1
    completion_order << [key, index]
  end

  finished.count_down
end

# Workload -------------------------------------------------------------------
#
# Twenty tasks for :a are posted first. They all serialize on the same key, so
# only one of the two workers can ever be busy with :a. The four :b tasks are
# posted afterwards but must still complete, interleaved with the :a stream.

20.times { |i| executor.post(:a) { run_task.call(:a, i) } }
4.times  { |i| executor.post(:b) { run_task.call(:b, i) } }

raise 'workload did not finish in time' unless finished.wait(10)

by_key = completion_order.group_by(&:first).transform_values { |pairs| pairs.map(&:last) }

puts "completion order: #{completion_order.map(&:first).join}"
puts "key :a ran in order: #{by_key[:a].inspect}"
puts "key :b ran in order: #{by_key[:b].inspect}"
puts "same-key overlaps observed: #{overlap_violations.value}"

# Assertions -----------------------------------------------------------------

unless by_key[:a] == (0...20).to_a && by_key[:b] == (0...4).to_a
  raise 'each key ran its tasks out of submission order'
end

unless overlap_violations.value.zero?
  raise 'two tasks for the same key ran at the same time'
end

# The first completion cannot be :b if :a were posted first... but some :b
# task must finish before the final :a task, proving key :b advanced while :a
# still had a long backlog (rather than waiting behind all twenty :a tasks).
last_a_position = completion_order.rindex { |key, _| key == :a }
b_progressed_before_a_finished =
  completion_order[0...last_a_position].any? { |key, _| key == :b }

raise 'busy key :a appears to have starved key :b' unless b_progressed_before_a_finished

# Keys are reclaimed once they become idle.
executor.shutdown
raise unless executor.wait_for_termination(5)
raise unless executor.key_count.zero?

puts
puts 'OK: same key serialized in order, different keys ran in parallel, ' \
     'busy :a did not starve :b, idle keys were reclaimed.'
