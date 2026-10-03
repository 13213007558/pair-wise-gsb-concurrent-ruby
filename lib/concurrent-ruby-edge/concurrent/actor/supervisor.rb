require 'concurrent/utility/monotonic_time'
require 'concurrent/actor/context'

module Concurrent
  module Actor
    # Starts and supervises a static, ordered collection of child actors.
    #
    # Children are started in child-spec order. With +:rest_for_one+, when a child
    # fails it and every child after it are terminated in reverse order, then
    # started again in spec order. Child references are looked up by id, so callers
    # observe the replacement only after the new child has initialized.
    #
    # Options include +:strategy+, +:max_restarts+, +:max_seconds+, and +:clock+.
    class Supervisor < Context
      STRATEGIES    = [:one_for_one, :one_for_all, :rest_for_one].freeze
      RESTART_TYPES = [:temporary, :transient, :permanent].freeze
      READY         = :__supervisor_ready__
      START         = :__supervisor_start__
      NEXT          = :__supervisor_next__
      STOPPED       = :__supervisor_stopped__

      module DefaultClock
        def self.now
          Concurrent.monotonic_time
        end
      end

      def self.spawn(name = :supervisor, children = [], options = {})
        spawn_options = {
          class: self,
          name:  name,
          args:  [children, options],
          link:  options.fetch(:link, true)
        }
        spawn_options[:executor] = options[:executor] if options.key?(:executor)
        spawn_options[:logger]   = options[:logger] if options.key?(:logger)
        Actor.spawn(spawn_options)
      end

      def self.spawn!(name = :supervisor, children = [], options = {})
        supervisor = spawn(name, children, options)
        supervisor.ask(READY).wait!
        supervisor
      rescue => error
        supervisor.ask(:terminate!) if supervisor
        raise error
      end

      def initialize(children, options = {})
        @child_specs  = normalize_children(children)
        @strategy     = options.fetch(:strategy, :one_for_one)
        @max_restarts = options.fetch(:max_restarts, 3)
        @max_seconds  = options.fetch(:max_seconds, 5)
        @clock        = options.fetch(:clock, DefaultClock)

        raise ArgumentError, "unknown strategy #{@strategy.inspect}" unless STRATEGIES.include?(@strategy)
        unless @max_restarts.is_a?(Integer) && @max_restarts >= 0
          raise ArgumentError, ':max_restarts must be a non-negative Integer'
        end
        unless @max_seconds.is_a?(Numeric) && @max_seconds > 0
          raise ArgumentError, ':max_seconds must be greater than 0'
        end
        unless @clock.respond_to?(:now)
          raise ArgumentError, ':clock must respond to :now'
        end

        @references      = {}
        @stopping        = {}
        @pending_ids     = []
        @starting_id     = nil
        @starting_ref    = nil
        @phase           = :initializing
        @restart_index   = nil
        @queued_failures = []
        @restart_times   = []
        @ready           = Promises.resolvable_future
        @ready_envelopes = []
        core.schedule_execution { tell START }
      end

      def on_message(message)
        case message
        when READY
          if @ready.resolved?
            resolve_ready_envelope(envelope)
          else
            @ready_envelopes << envelope
          end
          Behaviour::MESSAGE_PROCESSED
        when START
          start_initial_children
        when NEXT
          start_next_child
        when STOPPED
          start_restarted_children
        when :children
          @references.dup
        when :child_specs
          @child_specs.map(&:dup)
        when :restarting?
          @phase == :restarting
        when Exception
          child_failed(envelope.sender, message)
        when ::Array
          case message.first
          when :child_initialized
            child_initialized(message[1])
          when :child_failed
            child_failed(message[1], message[2])
          when :child_terminated
            child_terminated(message[1], message[2])
          when :child
            @references[message[1]]
          when :supervision_failed
            child_failed(envelope.sender, message[1])
          when :terminated
            child_terminated(envelope.sender, message[1])
          else
            pass
          end
        else
          pass
        end
      end

      private

      # A spec uses either :class/:args/:block, or :start which must return
      # [Reference, initialized_future]. The future is fulfilled after the child
      # initializes, or rejected when its initialization fails.
      def normalize_children(children)
        children.map do |child|
          spec = child.dup
          id   = spec.delete(:id) || spec[:name]
          raise ArgumentError, 'child spec must have an :id or :name' if id.nil?
          spec[:id]      = id
          spec[:name]    = id unless spec.key?(:name)
          spec[:restart] = spec.fetch(:restart, :permanent)
          unless RESTART_TYPES.include?(spec[:restart])
            raise ArgumentError, "unknown restart type #{spec[:restart].inspect}"
          end
          unless spec[:start].respond_to?(:call) || spec[:class]
            raise ArgumentError, "child #{id.inspect} must have :class or :start"
          end
          spec
        end.tap do |specs|
          ids = specs.map { |spec| spec[:id] }
          duplicates = ids.select { |id| ids.count(id) > 1 }.uniq
          raise ArgumentError, "duplicate child ids #{duplicates.inspect}" unless duplicates.empty?
        end
      end

      def start_initial_children
        @phase       = :starting
        @pending_ids = @child_specs.map { |spec| spec[:id] }
        tell NEXT
      end

      def start_next_child
        if @pending_ids.empty?
          children_started
          return true
        end

        id = @pending_ids.first
        child, started = start_child(child_spec(id))
        @starting_id  = id
        @starting_ref = child
        started.on_resolution! do |fulfilled, _value, error|
          tell [:child_initialized, id: id, reference: child, fulfilled: fulfilled, error: error]
        end
        true
      rescue => error
        tell [:child_initialized, id: @pending_ids.first, reference: nil, fulfilled: false, error: error]
      end

      def children_started
        @starting_id  = nil
        @starting_ref = nil
        if @phase == :starting
          @phase = :running
          @ready.fulfill reference unless @ready.resolved?
          @ready_envelopes.each { |ready_envelope| resolve_ready_envelope(ready_envelope) }
          @ready_envelopes.clear
        elsif @phase == :restarting
          if @queued_failures.empty?
            @phase = :running
          else
            failed_id = @queued_failures.min_by { |id| child_index(id) }
            @queued_failures.clear
            restart_after_queued_failures(failed_id)
          end
        end
      end

      def child_initialized(result)
        return true unless result[:id] == @starting_id

        @starting_id  = nil
        @starting_ref = nil

        if result[:fulfilled]
          @references[result[:id]] = result[:reference]
          @pending_ids.shift
          tell NEXT
        elsif @phase == :starting
          stop_all_and_escalate(result[:error])
        else
          child_start_failed(result[:id], result[:error])
        end
      end

      def child_start_failed(id, error)
        restart_failed_start(id, error)
        tell NEXT unless @phase == :escalating
      end

      def child_failed(child, error)
        return queue_failure(@starting_id) if @starting_ref == child && @phase == :restarting
        return true if @starting_ref == child
        spec = spec_for_reference(child) || @stopping[child]
        return true unless spec
        @stopping.delete(child)
        return terminate_temporary(child, spec) if spec[:restart] == :temporary
        return queue_failure(spec[:id]) if @phase == :restarting
        return true unless @phase == :running

        begin_restart(spec[:id])
      end

      def child_terminated(child, reason)
        return true if @starting_ref == child
        spec = spec_for_reference(child) || @stopping[child]
        return true unless spec
        @stopping.delete(child)

        @references.delete(spec[:id])
        return true unless @phase == :running
        return true if spec[:restart] == :temporary
        return true if spec[:restart] == :transient && reason.nil?

        begin_restart(spec[:id])
      end

      def begin_restart(failed_id)
        index = restart_index(failed_id)
        record_restart
        prepare_restart(index)
        true
      rescue SupervisionError => error
        escalate error
      end

      def restart_after_queued_failures(failed_id)
        index = restart_index(failed_id)
        record_restart
        prepare_restart(index)
        true
      rescue SupervisionError => error
        escalate error
      end

      def prepare_restart(index)
        @phase          = :restarting
        @restart_index   = index
        @queued_failures = []
        @stopping.clear
        ids             = @child_specs[index..-1].map { |spec| spec[:id] }
        stopping_pairs = ids.each_with_index.map do |id, offset|
          child = @references[id]
          [child, @child_specs[index + offset]] if child
        end.compact
        references = stopping_pairs.map(&:first)
        stopping_pairs.each { |child, spec| @stopping[child] = spec }
        ids.each { |id| @references.delete(id) }
        stop_reference(references.last, references[0..-2])
      end

      def stop_reference(child, remaining)
        if child.nil?
          tell STOPPED
        else
          terminated = child.ask(:termination_event)
          child << [:terminate!, :kill]
          terminated.on_resolution! { stop_reference(remaining.last, remaining[0..-2]) }
        end
      end

      def start_restarted_children
        @stopping.clear
        @pending_ids = @child_specs[@restart_index..-1].map { |spec| spec[:id] }
        tell NEXT
      end

      def restart_failed_start(failed_id, error)
        index = child_index(failed_id)
        record_restart
        ensure_restart_allowed
        @pending_ids = @child_specs[index..-1].map { |spec| spec[:id] }
      rescue SupervisionError => supervision_error
        escalate supervision_error
      end

      def start_child(spec)
        if spec[:start].respond_to?(:call)
          child, started = start_without_current_block(spec, @references.dup, &nil)
          Type! child, Reference
          unless started.respond_to?(:wait)
            raise ArgumentError, "start for #{spec[:id].inspect} must return [reference, future]"
          end
          [child, started]
        else
          start_spec(spec)
        end
      end

      def start_spec(spec)
        options     = spec.reject { |key, _| [:id, :restart, :start, :class].include?(key) }
        block       = options.delete(:block)
        args        = options.delete(:args) || []
        args        = args.call(@references.dup) if args.respond_to?(:call)
        initialized = Promises.resolvable_future
        spawn_options = options.merge(class: spec[:class], args: args, initialized: initialized)
        child = if block
                  Actor.spawn(spawn_options, &block)
                else
                  spawn_without_current_block(spawn_options)
                end
        [child, initialized]
      end

      def record_restart
        now = @clock.now.to_f
        @restart_times.push now
        @restart_times.select! { |time| time > now - @max_seconds.to_f }
        ensure_restart_allowed
      end

      def ensure_restart_allowed
        raise SupervisionError.new(core.reference) if @restart_times.length > @max_restarts
      end

      def spawn_without_current_block(*arguments)
        Actor.spawn(*arguments)
      end

      def start_without_current_block(spec, references)
        call_start_without_block(spec[:start], references)
      end

      def call_start_without_block(start, references)
        start.call(references, &nil)
      end

      def stop_all_and_escalate(error)
        @phase     = :stopping
        references = @references.values
        @references.clear
        references.reverse!
        stop_all_next(references, error)
      end

      def stop_all_next(references, error)
        if references.empty?
          escalate error
        else
          child = references.shift
          terminated = child.ask(:termination_event)
          child << [:terminate!, :kill]
          terminated.on_resolution! { stop_all_next(references, error) }
        end
        true
      end

      def escalate(error)
        @phase = :escalating
        unless @ready.resolved?
          @ready.reject error
          @ready_envelopes.each { |ready_envelope| ready_envelope.future.reject error }
          @ready_envelopes.clear
        end
        core.parent << [:supervision_failed, error] if core.parent
        terminate! error
      end

      def resolve_ready_envelope(ready_envelope)
        if @ready.fulfilled?
          ready_envelope.future.fulfill @ready.value
        else
          ready_envelope.future.reject @ready.reason
        end
      end

      def terminate_temporary(child, spec)
        @references.delete(spec[:id])
        child << [:terminate!, :kill]
        true
      end

      def queue_failure(id)
        @queued_failures << id unless @queued_failures.include?(id)
        true
      end

      def restart_index(id)
        @strategy == :one_for_all ? 0 : child_index(id)
      end

      def child_index(id)
        @child_specs.index { |spec| spec[:id] == id } or raise KeyError, "unknown child #{id.inspect}"
      end

      def child_spec(id)
        @child_specs[child_index(id)]
      end

      def spec_for_reference(reference)
        @child_specs.find { |spec| @references[spec[:id]] == reference }
      end
    end
  end
end
