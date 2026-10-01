require 'fiber'
require 'concurrent/constants'
require_relative 'atomic/locals'

module Concurrent
  # An explicitly registered, thread-local value that may be propagated to
  # tasks submitted to thread pools.
  #
  # Unlike copying all of `Thread.current`, only values held by registered
  # `ContextVar`s are ever propagated. A value is captured on the submitting
  # thread when a task is posted (see {Context.capture} and
  # {ContextPropagatingExecutor}), installed on the worker thread for the
  # duration of the task, and the worker thread's previous values are always
  # restored when the task finishes - even when the task raises, when it is
  # rejected and run by the caller, or when it runs on an inline executor.
  #
  # A worker thread reused for two unrelated requests therefore can never see
  # the context of the previous request.
  #
  # @example Simple propagation
  #   trace_id = Concurrent::ContextVar.new(:trace_id)
  #   pool     = Concurrent::FixedThreadPool.new(1)
  #   executor = Concurrent::ContextPropagatingExecutor.new(pool)
  #
  #   trace_id.value = 'abc'
  #   latch = Concurrent::CountDownLatch.new
  #   executor.post { expect(trace_id.value).to eq('abc'); latch.count_down }
  #   latch.wait
  #
  # @example Copying mutable values into each task
  #   request_headers = Concurrent::ContextVar.new(:headers, dup_on_enter: true)
  class ContextVar
    # @!visibility private
    STORAGE = ThreadLocals.new

    # @return [Object] the key the variable is registered with, defaults to the
    #   object itself
    attr_reader :name

    # Creates and registers a new context variable.
    #
    # @param [Object] name optional key identifying the variable (e.g. a Symbol
    #   or String). Defaults to the variable itself.
    # @param [Object] default the value returned by {#value} when nothing has
    #   been set on the current thread
    # @param [Boolean] dup_on_enter when `true`, {Context.capture} calls `#dup`
    #   on the value so a task cannot mutate the submitting thread's object
    # @param [Proc, nil] copy_on_enter when given, {Context.capture} calls the
    #   proc with the value and propagates its result. Takes precedence over
    #   `dup_on_enter`.
    # @yield optional block called (on the reading thread) to obtain a default
    #   value, like {ThreadLocalVar}
    # @raise [ArgumentError] when both a default value and a default block are
    #   given
    def initialize(name = nil, default = nil, dup_on_enter: false, copy_on_enter: nil, &default_block)
      if default && block_given?
        raise ArgumentError, 'Cannot use both value and block as default value'
      end

      @name          = name.nil? ? self : name
      @default       = default
      @default_block = block_given? ? default_block : nil
      @dup_on_enter  = dup_on_enter
      @copy_on_enter = copy_on_enter
      @index         = STORAGE.next_index(self)

      Context.register(self)
    end

    # @return [Object] the value bound to the variable on the current thread
    def value
      STORAGE.fetch(@index) { default }
    end

    # Binds a value to the variable on the current thread.
    #
    # @param [Object] value the new value
    # @return [Object] the new value
    def value=(value)
      STORAGE.set(@index, value)
    end

    # Runs the given block with the variable bound to `value` on the current
    # thread, restoring the previous binding afterwards.
    #
    # @param [Object] value the value to bind for the duration of the block
    # @return [Object] the block result
    def bind(value)
      raise ArgumentError, 'no block given' unless block_given?
      old = self.value
      self.value = value
      begin
        yield
      ensure
        self.value = old
      end
    end

    # @!visibility private
    # @return [Object] the raw value stored on the current thread, or
    #   {Context::UNSET} when the variable was never set on it
    def __raw_value__
      locals = STORAGE.locals
      return Context::UNSET if locals.nil?
      stored = locals[@index]
      stored.nil? ? Context::UNSET : (NULL.equal?(stored) ? nil : stored)
    end

    # @!visibility private
    # @param [Object] value a raw value (nil is stored as actual nil), or
    #   {Context::UNSET} to remove the binding from the current thread
    def __set_raw_value__(value)
      if Context.unset?(value)
        locals = STORAGE.locals
        locals[@index] = nil if locals
      else
        STORAGE.set(@index, value)
      end
    end

    # @!visibility private
    # @return [Object] the value as it should be stored in a propagation
    #   snapshot, applying the configured copy policy
    def __copy_for_snapshot__(value)
      if @copy_on_enter
        @copy_on_enter.call(value)
      elsif @dup_on_enter
        value.dup
      else
        value
      end
    end

    def to_s
      "#<#{self.class}: #{@name.inspect}>"
    end
    alias_method :inspect, :to_s

    protected

    # @!visibility private
    def default
      if @default_block
        self.value = @default_block.call
      else
        @default
      end
    end
  end

  # Propagation of explicitly registered {ContextVar}s across thread pool
  # boundaries.
  #
  # @see ContextVar
  # @see ContextPropagatingExecutor
  module Context
    # Sentinel describing "no value is bound on this thread".
    UNSET = Object.new.freeze

    @registry = ObjectSpace::WeakMap.new
    @registry_lock = Mutex.new

    class << self
      # @!visibility private
      def register(variable)
        @registry_lock.synchronize { @registry[variable] = true }
      end

      # @param [Object] value
      # @return [Boolean] true when `value` is the "unset" sentinel
      # @!visibility private
      def unset?(value)
        UNSET.equal?(value)
      end

      # Captures the values of all registered context variables which are set on
      # the current thread.
      #
      # Each value is copied according to the variable's configuration
      # (`dup_on_enter` / `copy_on_enter`). Variables that are not set on the
      # current thread are not part of the snapshot. Variables created after the
      # snapshot is taken are never part of it.
      #
      # @return [Snapshot]
      def capture
        snapshot(copy: true)
      end

      # Installs a snapshot on the current thread. Returns a restorable snapshot
      # of the bindings that were present beforehand; pass it to {restore} once
      # the task is done.
      #
      # The previous bindings are captured verbatim (no copy policy is applied)
      # so {restore} reinstates exactly the objects the thread had before.
      #
      # @param [Snapshot] snapshot
      # @return [Snapshot] the previous bindings
      def apply(snapshot)
        previous = snapshot(copy: false)
        snapshot.__bindings__.each do |variable, value|
          variable.__set_raw_value__(value)
        end
        previous
      end

      # Restores bindings captured by {apply}. Variables that were set by the
      # task but were absent from the previous bindings are explicitly unset, so
      # no value leaks onto a reused worker thread.
      #
      # @param [Snapshot] previous the snapshot returned by {apply}
      def restore(previous)
        previous_bindings = previous.__bindings__
        each_registered do |variable|
          if previous_bindings.key?(variable)
            variable.__set_raw_value__(previous_bindings[variable])
          else
            variable.__set_raw_value__(UNSET)
          end
        end
      end

      # Runs a block with the given (or freshly captured) snapshot installed,
      # always restoring the previous bindings afterwards.
      #
      # @param [Snapshot, nil] snapshot the snapshot to install; captures the
      #   current context when `nil`
      # @return [Object] the block result
      def with_snapshot(snapshot = nil)
        raise ArgumentError, 'no block given' unless block_given?
        snapshot ||= capture
        previous = apply(snapshot)
        begin
          yield
        ensure
          restore(previous)
        end
      end

      private

      def snapshot(copy:)
        bindings = {}
        each_registered do |variable|
          value = variable.__raw_value__
          next if unset?(value)
          bindings[variable] = copy ? variable.__copy_for_snapshot__(value) : value
        end
        Snapshot.new(bindings)
      end

      def each_registered
        @registry_lock.synchronize do
          variables = []
          @registry.each_key { |variable| variables << variable }
          variables
        end.each { |variable| yield variable }
      end
    end

    # An immutable copy of the bindings of registered {ContextVar}s taken at a
    # single point in time (task submission).
    class Snapshot
      # @!visibility private
      def initialize(bindings)
        @__bindings__ = bindings.freeze
      end

      # @!visibility private
      attr_reader :__bindings__

      # @return [Boolean] true when the snapshot carries no bindings
      def empty?
        @__bindings__.empty?
      end

      # @return [Object] the value stored for the given variable in the snapshot
      def [](variable)
        @__bindings__[variable]
      end

      def size
        @__bindings__.size
      end
    end
  end
end
