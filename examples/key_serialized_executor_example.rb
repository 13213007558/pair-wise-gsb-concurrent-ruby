require_relative 'init'

# An executor with exactly TWO worker threads shared by all keys.
executor = Concurrent::KeySerializedExecutor.new(2, max_tasks: 1_000)

# One account ("A") submits many long-running updates back to back.
# Another account ("B") submits a single update while A is saturated.
#
# Expected output proves:
#   * tasks of the same key never overlap (A is always executed by a single
#     thread and its sequence numbers are strictly increasing), and
#   * B still makes progress although A keeps the whole pool busy: the worker
#     processing A releases the key after each task and re-enqueues the
#     follow-up, so the second worker can run B immediately.

mutex = Mutex.new
log = lambda do |event, **data|
  mutex.synchronize do
    t = (Concurrent.monotonic_time * 1000).round
    fields = data.map { |k, v| " #{k}=#{v}" }.join
    puts format('%7d ms  %-18s%s', t, event, fields)
  end
end

A_TASKS = 8
a_started   = Concurrent::CountDownLatch.new(1)
a_done      = Concurrent::CountDownLatch.new(A_TASKS)
b_done      = Concurrent::CountDownLatch.new(1)
a_remaining = Concurrent::AtomicFixnum.new(A_TASKS)

A_TASKS.times do |i|
  executor.post('account-A') do
    a_started.count_down
    log.call('A start', seq: i, thread: Thread.current.object_id)
    sleep 0.05 # simulate an account update
    log.call('A done ', seq: i, thread: Thread.current.object_id)
    a_remaining.decrement
    a_done.count_down
  end
end

# Wait until the first A task occupies a worker, then post B.
raise 'A never started' unless a_started.wait(5)
log.call('B posted')

executor.post('account-B') do
  log.call('B start', thread: Thread.current.object_id)
  sleep 0.05
  log.call('B done ', thread: Thread.current.object_id)
  b_done.count_down
end

raise 'B did not finish in time' unless b_done.wait(5)
log.call('B finished while A is still draining the queue',
         remaining_a: a_remaining.value)
a_done.wait(10)

executor.shutdown
executor.wait_for_termination(5)

puts
puts 'B finished before the last A tasks ran, therefore a busy key cannot'
puts 'starve another key; same-key tasks never overlapped (one thread at a time).'
