require 'concurrent/constants'

module Concurrent
  # A {ContextVar} is a {ThreadLocalVar}-like variable which is explicitly
  # registered for propagation to tasks executed on other threads by
  # {Concurrent}'s executors.
  #
  # Ruby code commonly uses thread local variables to store request scoped
  # values such as a tracing id. When work is submitted to a thread pool the
  # values live on the submitting thread, so they are not available in the
  # worker thread. Copying *all* the thread local variables would carry over
  # state which is not meant to cross the boundary (other libraries, internal
  # bookkeeping, etc.). A {ContextVar} only propagates when it has been
  # explicitly registered, and it only carries its own value.
  #
  # The value is captured when a task is submitted. When the task starts
  # running on a worker thread the captured value is installed, and after the
  # task finishes - whether it returns, raises or is rejected - the worker
  # thread's original value is restored, leaving no residue for the next task
  # processed by the same thread.
  #
  # How the captured value is copied is decided by the `:copy` option:
  #
  # * `:none` (default) the exact same object is used. Appropriate for
  #   immutable values such as `Symbol` and frozen objects.
  # * `:dup` the value is duplicated with `Object#dup` when captured.
  # * `:clone` the value is cloned with `Object#clone` when captured.
  # * `:marshal` the value is deep-copied with `Marshal.dump`/`Marshal.load`.
  # * a `Proc` (anything responding to `#call`) is called with the value and
  #   has to return the copy.
  #
  # @example
  #   trace_id = Concurrent::ContextVar.new(:trace_id)
  #
  #   trace_id.value = 'request-A'
  #   pool   = Concurrent::FixedThreadPool.new(1)
  #   future = Concurrent::Promises.future_on(pool) { trace_id.value }
  #   future.value! # => 'request-A'
  #
  #   # the worker thread is restored after every task
  #   Concurrent::Promises.future_on(pool) { trace_id.value }.value! # => nil
  #
  # @see Concurrent::Context
  class ContextVar
    # Strategies for copying a value when it is captured.
    # @!visibility private
    module Copy
      def self.none(value)
        value
      end

      def self.dup(value)
        value.dup
      end

      def self.clone(value)
        value.clone
      end

      def self.marshal(value)
        ::Marshal.load(::Marshal.dump(value))
      end

      IMPLEMENTATIONS = {
        none:      method(:none),
        reference: method(:none),
        dup:       method(:dup),
        clone:     method(:clone),
        marshal:   method(:marshal),
      }.freeze
    end
    private_constant :Copy

    class << self
      # Registers a variable so it is included in context snapshots.
      # @!visibility private
      def register(variable)
        Context.register(variable)
      end
    end

    # @return [Object] an optional name used for inspection only
    attr_reader :name

    # Creates a context variable and explicitly registers it for propagation.
    #
    # @param [Object] name optional name, useful for logging and {#inspect}
    # @param [Object] default the value returned by {#value} when nothing has
    #   been set on the current thread
    # @param [Proc] default_block optional block called to produce the default
    #   value lazily for each thread (a positional block is accepted too)
    # @param [Symbol, #call] copy how captured values are copied, see the
    #   class documentation for the supported strategies
    #
    # @raise [ArgumentError] if both `default` and a block are given
    # @raise [ArgumentError] if `copy` is not a known strategy
    def initialize(name = nil, default: nil, default_block: nil, copy: :none, &block)
      default_block ||= block
      if default_block
        raise ArgumentError, 'cannot use both default and default_block' unless default.nil?
        @default_block = default_block
        @default       = nil
      else
        @default_block = nil
        @default       = default
      end

      @copier =
        if copy.respond_to?(:call) && !copy.is_a?(Symbol)
          copy
        else
          Copy::IMPLEMENTATIONS.fetch(copy || :none) do
            raise ArgumentError, "unknown copy strategy #{copy.inspect}"
          end
        end

      @name  = name
      @index = Context.__register(self)
    end

    # Returns the value in the current thread's copy of the variable, or the
    # default value when it has never been set.
    #
    # @return [Object] the current value
    def value
      raw = __read_slot
      if raw.nil?
        default_value
      elsif raw.equal?(NULL)
        nil
      else
        raw
      end
    end

    # Sets the current thread's copy of the variable to the given value.
    #
    # @param [Object] value the new value
    # @return [Object] the new value
    def value=(value)
      __write_slot(value.nil? ? NULL : value)
      value
    end

    # Binds the given value for the duration of the block, restoring the
    # previous value (or unsetting it) afterwards.
    #
    # @param [Object] value the value to bind
    # @yield operation performed with the bound value
    # @return [Object] the result of the block
    def bind(value)
      raise ArgumentError, 'a block is required' unless block_given?
      was_set  = set?
      previous = __read_slot
      self.value = value
      begin
        yield
      ensure
        was_set ? __write_slot(previous) : clear
      end
    end

    # Removes the value bound to the current thread. Subsequent {#value} calls
    # return the default again.
    #
    # @return [nil]
    def clear
      Context.__storage[@index] = nil
      nil
    end

    # Whether a value has been set on the current thread.
    #
    # @return [Boolean]
    def set?
      !__read_slot.nil?
    end

    # @return [String]
    def inspect
      "#<#{self.class}: #{name || @index} current=#{value.inspect}>"
    end
    alias_method :to_s, :inspect

    # @!visibility private
    # Returns the value to capture into a snapshot, or {Context::UNSET} when
    # nothing has been set on the current thread.
    def capture
      return Context::UNSET unless set?
      @copier.call(value)
    end

    # @!visibility private
    # Installs a captured value on the current thread.
    def install(captured)
      if captured.equal?(Context::UNSET)
        clear
      else
        self.value = captured
      end
    end

    # @!visibility private
    # Reads the raw slot: nil means unset, {NULL} means explicitly set nil.
    def __read_slot
      Context.__storage[@index]
    end

    # @!visibility private
    # Writes the raw slot, preserving the {NULL}/nil distinction.
    def __write_slot(raw)
      Context.__storage[@index] = raw
    end

    private

    def default_value
      if @default_block
        value = @default_block.call
        self.value = value
        value
      else
        @default
      end
    end
  end

  # {Context} is the registry of {ContextVar context variables} and the entry
  # point for capturing the variables into snapshots which are installed while
  # a task runs on another thread.
  #
  # The executors shipped with {Concurrent} capture the context of the
  # submitting thread automatically in `#post`. Continuations created with
  # {Concurrent::Promises::Future#then then} opt in explicitly, and they
  # capture the context at the moment the continuation is registered.
  #
  # @see Concurrent::ContextVar
  module Context
    # Placeholder stored in a snapshot for a variable which was not set on the
    # capturing thread. Installing it clears the variable rather than setting
    # it to nil.
    UNSET = Concurrent::NULL

    @registry_mutex = Mutex.new
    # The registered variables, indexed by their monotonically allocated
    # slots. The registry is intentionally strong: a context variable is
    # explicitly registered and expected to live for the application
    # (e.g. a tracing id defined once at boot). Values remain per-thread and
    # are reclaimed with their threads; a variable without a value on a
    # thread occupies a single nil slot.
    @variables      = []
    @next_slot      = 0
    # Thread => per-thread values array. Only []/[]= are used, available on
    # every supported Ruby. Keys are weak (dead threads drop out) while the
    # thread itself keeps the array alive strongly through a thread local.
    @all_storage    = ObjectSpace::WeakMap.new
    # Thread local key holding the strong reference to the per-thread array.
    STORAGE_KEY     = :concurrent_context_storage

    class << self
      # Registers a {ContextVar}.
      #
      # @return [Integer] the slot assigned to the variable
      def register(variable)
        @registry_mutex.synchronize do
          index = @next_slot
          @next_slot += 1
          @variables[index] = variable
          index
        end
      end

      # @return [Array<ContextVar>] the registered variables, ordered by
      #   registration
      def registered
        @registry_mutex.synchronize { @variables.dup }
      end

      # Whether any variables are currently registered.
      # @return [Boolean]
      def empty?
        @registry_mutex.synchronize { @variables.any? }
      end

      # Captures the values of all registered variables on the current thread.
      #
      # @return [Snapshot]
      def capture
        Snapshot.capture
      end

      # @return [Snapshot] an empty snapshot which clears all registered
      #   variables while active
      def empty
        Snapshot.empty
      end

      # Captures the current context and wraps a task so that the captured
      # values are installed for the duration of the task on the executing
      # thread. The executing thread's own values are always restored after
      # the task, even when it raises.
      #
      # @yield the task to wrap
      # @return [Bound] the wrapped task
      def wrap(&task)
        raise ArgumentError, 'a block is required' unless block_given?
        capture.bind(&task)
      end

      # Wraps a task with an explicitly supplied snapshot. The resulting task
      # is recognized by the executors and is not wrapped again, so it can be
      # used to make a task run with a context different from the submitting
      # thread's.
      #
      # @param [Snapshot] snapshot
      # @yield the task to wrap
      # @return [Bound] the wrapped task
      def bind(snapshot, &task)
        raise ArgumentError, 'a block is required' unless block_given?
        snapshot.bind(&task)
      end

      # Runs a block with the given snapshot installed, restoring the current
      # thread's values afterwards.
      #
      # @param [Snapshot] snapshot
      # @return [Object] the block result
      def activate(snapshot = capture)
        raise ArgumentError, 'a block is required' unless block_given?
        snapshot.activate { yield }
      end

      # Resolves a user supplied context option into a snapshot.
      # @!visibility private
      #
      # @option option [nil] no explicit snapshot (used at submission time,
      #   the executors capture the submitting context themselves)
      # @option option [true] capture the current context now
      # @option option [false] isolate with an empty snapshot
      # @option option [Snapshot] bind that snapshot
      def snapshot_option(option)
        case option
        when nil     then nil
        when true    then capture
        when false   then empty
        when Snapshot then option
        else
          raise ArgumentError, "invalid context #{option.inspect}"
        end
      end

      # Wraps a task for execution unless it already carries a snapshot. Used
      # by the executors.
      # @!visibility private
      def propagate(task)
        return task if Bound === task
        wrap(&task)
      end

      # @!visibility private
      # The per-thread array holding the raw values for registered slots.
      def __storage
        thread = Thread.current
        storage = thread.thread_variable_get(STORAGE_KEY)
        unless storage
          storage = []
          thread.thread_variable_set(STORAGE_KEY, storage)
          @all_storage[thread] = storage
        end
        storage
      end

      # @!visibility private
      def __register(variable)
        register(variable)
      end
    end

    # An immutable copy of the values of all registered variables at the
    # moment of capture.
    class Snapshot
      # @return [Array] array indexed by variable slot; each entry is a
      #   `[ContextVar, captured]` pair, or nil for a free slot.
      #   {Context::UNSET} marks variables which were not set.
      attr_reader :entries
      protected :entries

      def initialize(entries)
        @entries = entries || []
        @entries.freeze
      end

      # @return [Snapshot] a snapshot which clears every registered variable
      #   while active (used to isolate a task from the running thread)
      def self.empty
        entries = Context.registered.map { |variable| [variable, UNSET] }
        new(entries)
      end

      # @return [Snapshot]
      def self.capture
        variables = Context.registered
        entries = Array.new(variables.length)
        variables.each_with_index do |variable, index|
          entries[index] = [variable, variable.capture] if variable
        end
        new(entries)
      end

      # Whether the snapshot carries a set value for a variable.
      # @return [Boolean]
      def set?(variable)
        pair = find(variable)
        !!pair && !pair[1].equal?(UNSET)
      end

      # @return [Object] the captured value for a variable, or its default
      def [](variable)
        pair = find(variable)
        if pair.nil? || pair[1].equal?(UNSET)
          variable.send(:default_value)
        else
          pair[1]
        end
      end

      # @return [Boolean] whether the snapshot carries any set value (as
      #   opposed to only {UNSET} entries)
      def empty?
        @entries.none? { |pair| pair && !pair[1].equal?(UNSET) }
      end

      # @return [Bound] a task which installs this snapshot while it runs
      def bind(&task)
        raise ArgumentError, 'a block is required' unless block_given?
        Bound.new(self, task)
      end

      # Installs the snapshot on the current thread for the duration of the
      # block, restoring every variable to its previous value (or clearing it)
      # afterwards. Variables which appear on the thread while the block runs
      # are cleared, so no value leaks out.
      #
      # @return [Object] the block result
      def activate
        raise ArgumentError, 'a block is required' unless block_given?

        # Record THIS thread's own values first. The snapshot was captured on
        # the submitting thread, but restore has to target the executing
        # thread, whose slots start empty (or hold a previous request's value
        # until it was restored).
        storage   = Context.__storage
        originals = storage.dup

        begin
          @entries.each do |pair|
            next unless pair
            variable, captured = pair
            variable.install(captured)
          end
          yield
        ensure
          current = Context.__storage
          # Blank every slot, then put the executing thread's originals back.
          current.each_index { |i| current[i] = nil }
          originals.each_with_index { |raw, i| current[i] = raw }
        end
      end

      # @return [String]
      def inspect
        pairs = @entries.compact.map { |variable, value| "#{variable.name || variable.__id__}=#{value.inspect}" }
        "#<#{self.class} #{pairs.join(', ')}>"
      end

      private

      def find(variable)
        @entries.each do |pair|
          return pair if pair && pair[0].equal?(variable)
        end
        nil
      end
    end

    # A task which carries the snapshot to install while it runs. The executors
    # recognize it and do not capture the context again.
    class Bound
      # @return [Snapshot]
      attr_reader :snapshot

      def initialize(snapshot, task)
        @snapshot = snapshot
        @task     = task
      end

      # Installs the snapshot for the duration of the wrapped task.
      def call(*args, &block)
        @snapshot.activate { @task.call(*args, &block) }
      end

      # @return [Proc] the task as a proc
      def to_proc
        @proc ||= method(:call).to_proc
      end
    end
  end
end
