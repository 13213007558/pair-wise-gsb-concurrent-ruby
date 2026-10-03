#!/usr/bin/env ruby
# frozen_string_literal: true

# Asynchronous parallel prefix scan with a NON-COMMUTATIVE operation:
# multiplication of a chain of 2x2 matrices (A*B != B*A in general).
#
# The example
#   * verifies the parallel scan against a serial left fold,
#   * counts every merge (operation application) with an AtomicFixnum,
#   * traces which task runs on which worker thread and in which order,
#   * prints the task dependency graph, total work, and critical path.
#
# Run with:  ruby -I lib/concurrent-ruby examples/scan_non_commutative.rb

require 'concurrent'

N      = 64   # elements in the chain
CHUNKS = 8    # chunks the input is split into
WORKERS = 4   # bounded executor size

# ---------------------------------------------------------------- matrices --

Matrix2x2 = Struct.new(:a, :b, :c, :d) do
  def to_s
    "[#{a} #{b}; #{c} #{d}]"
  end
end

IDENTITY = Matrix2x2.new(1, 0, 0, 1).freeze

# Associative, non-commutative 2x2 matrix multiplication.
def multiply(left, right)
  Matrix2x2.new(
    left.a * right.a + left.b * right.c, left.a * right.b + left.b * right.d,
    left.c * right.a + left.d * right.c, left.c * right.b + left.d * right.d
  )
end

# ------------------------------------------------------- merge counting ----

$merge_count = Concurrent::AtomicFixnum.new(0)

def counted_multiply(left, right)
  $merge_count.increment
  multiply(left, right)
end

# ---------------------------------------------------- task tracing ---------

# Logs every task the scan schedules: when it starts/ends and on which
# worker thread. The scan only requires the executor to respond to #post.
class TracingExecutor
  include Concurrent::ExecutorService

  Event = Struct.new(:task, :phase, :thread, :at)

  attr_reader :events

  def initialize(inner)
    @inner  = inner
    @events = Concurrent::Array.new
    @seq    = Concurrent::AtomicFixnum.new(0)
    @t0     = Concurrent.monotonic_time
  end

  def post(*args, &task)
    id = @seq.increment
    @inner.post(*args) do
      @events << Event.new(id, :start, Thread.current.object_id, Concurrent.monotonic_time - @t0)
      task.call(*args)
    ensure
      @events << Event.new(id, :end, Thread.current.object_id, Concurrent.monotonic_time - @t0)
    end
  end
end

# ------------------------------------------------------------------- run ---

rng      = Random.new(2026)
matrices = Array.new(N) { Matrix2x2.new(rng.rand(-4..4), rng.rand(-4..4), rng.rand(-4..4), rng.rand(-4..4)) }

pool    = Concurrent::FixedThreadPool.new(WORKERS)
tracing = TracingExecutor.new(pool)

$merge_count.value = 0
scan_future = Concurrent::Scan.inclusive_scan(matrices, identity: IDENTITY, executor: tracing, chunks: CHUNKS) do |acc, m|
  counted_multiply(acc, m)
end
parallel        = scan_future.value!(30)
parallel_merges = $merge_count.value

# Serial reference (and its merge count).
$merge_count.value = 0
serial = matrices.each_with_object([]) do |m, out|
  out << (out.empty? ? counted_multiply(IDENTITY, m) : counted_multiply(out.last, m))
end
serial_merges = $merge_count.value

pool.shutdown
pool.wait_for_termination(10)

# ------------------------------------------------------------- report ------

puts "non-commutative check: A*B == B*A? #{multiply(matrices[0], matrices[1]) == multiply(matrices[1], matrices[0])}"
puts "parallel scan == serial scan?      #{parallel == serial}"
puts "prefixes returned (in order):      #{parallel.size}"
puts "last prefix == total product?      #{parallel.last == serial.last}"
puts

puts "merges (operation applications):"
puts "  serial scan:   #{serial_merges}  (= n merges including the identity seed)"
puts "  parallel scan: #{parallel_merges}  (= (n - c) totals + (c - 1) prefixes + n rescans = 2n - 1 = #{2 * N - 1})"
puts

puts 'task dependency graph (all tasks posted to the bounded executor):'
puts '  totals[0..c-1]  (c independent chunk folds, one task per chunk)'
puts '       |'
puts '       v'
puts '  chunk-prefixes  (1 task: exclusive scan of the c totals, in index order)'
puts '       |'
puts '       v'
puts '  rescan[0..c-1]  (c independent tasks, each seeded with its chunk prefix)'
puts '       |'
puts '       v'
puts '  join            (1 task: concatenate chunk outputs in index order)'
puts

puts "total work:    O(n)  -- 2n - 1 merges for n=#{N}, c=#{CHUNKS}"
puts "critical path: O(n/c + c) -- longest chunk fold (#{N / CHUNKS}) + chunk prefix pass (#{CHUNKS - 1}) + longest rescan (#{N / CHUNKS})"
puts '               vs O(n) for a serial scan; more chunks than workers only queues tasks, never blocks them.'
puts

puts 'execution trace (task id, phase, worker thread, seconds since start):'
tracing.events.sort_by(&:at).each do |event|
  puts format('  task %2d  %-5s  thread %#x  t=%.4f', event.task, event.phase, event.thread, event.at)
end
