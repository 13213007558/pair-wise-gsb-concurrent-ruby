require 'concurrent/actor'
require 'timeout'

module Concurrent
  module Actor
    RSpec.describe 'Actor :rest_for_one supervision', edge: true do

      # A fake worker actor which depends on the workers started before it.
      # It records its starts (together with the dependencies it saw) and its stops
      # into a shared log. Through a shared controls hash it can be told to fail
      # on start, to fail right after (re)start, or to block a start on a gate.
      class DependentWorker < RestartingContext
        def initialize(log, name, siblings, controls)
          @log      = log
          @name     = name
          @siblings = siblings
          @controls = controls
          @log << [:start, name, siblings.keys]
          if @controls[:start_failures][name] > 0
            @controls[:start_failures][name] -= 1
            raise "start of #{name} failed"
          end
          @siblings[name] = reference
          gate            = @controls[:start_gate]
          gate.pop if gate && @controls[:gated_child] == name
          reference.tell :fail if @controls[:fail_after_start][name]
        end

        def on_message(message)
          case message
          when :fail
            raise "boom from #{@name}"
          else
            message
          end
        end

        def on_event(event)
          @log << [:stop, @name] if event.is_a?(Array) && event.first == :terminated
        end
      end

      def new_controls
        { start_failures:   Hash.new(0),
          fail_after_start: Hash.new(false),
          start_gate:       nil,
          gated_child:      :c }
      end

      def build_supervisor(log, siblings, controls, options)
        Utils::AdHoc.spawn! name:                 :rest_for_one_supervisor,
                            args:                 [log, siblings, controls],
                            behaviour_definition: Behaviour.restarting_behaviour_definition(
                              :restart!, :rest_for_one, **options) do |shared_log, shared_siblings, shared_controls|
          DependentWorker.spawn! name: :a, args: [shared_log, :a, shared_siblings, shared_controls]
          DependentWorker.spawn! name: :b, args: [shared_log, :b, shared_siblings, shared_controls]
          DependentWorker.spawn! name: :c, args: [shared_log, :c, shared_siblings, shared_controls]
          -> message { message == :children ? children : nil }
        end
      end

      # Spawns a plain parent actor which only logs the messages it receives,
      # with the :rest_for_one supervisor as its child.
      # @return [Array(Reference, Hash)] the parent and a holder with the supervisor reference
      def spawn_supervised_tree(log, siblings, controls, parent_log, options, holder = {})
        parent = Utils::AdHoc.spawn! name: :parent,
                                     args: [log, siblings, controls, parent_log, options, holder] do
          |shared_log, shared_siblings, shared_controls, shared_parent_log, shared_options, shared_holder|
          shared_holder[:supervisor] = build_supervisor(shared_log, shared_siblings, shared_controls, shared_options)
          -> message { shared_parent_log << message }
        end
        [parent, holder]
      end

      def terminate_actors(*actors)
        actors.each do |actor|
          actor.ask!(:terminate!) unless actor.ask!(:terminated?)
        end
      end

      def wait_until(timeout = 5)
        Timeout.timeout(timeout) { sleep 0.01 until yield }
      end

      it 'stops the failed child and its successors in reverse order and restarts them in registration order' do
        log      = Queue.new
        siblings = {}
        controls = new_controls

        supervisor = build_supervisor(log, siblings, controls,
                                      max_restarts: 10, within_seconds: 60)

        # children start in registration order, later children see earlier ones as dependencies
        expect(log.pop).to eq [:start, :a, []]
        expect(log.pop).to eq [:start, :b, [:a]]
        expect(log.pop).to eq [:start, :c, [:a, :b]]

        original = siblings.dup

        # the middle child fails
        siblings[:b] << :fail

        # the failed child and its successors are stopped in reverse order ...
        expect(log.pop).to eq [:stop, :c]
        expect(log.pop).to eq [:stop, :b]
        # ... and started again in registration order, :a is never touched
        expect(log.pop[0..1]).to eq [:start, :b]
        expect(log.pop[0..1]).to eq [:start, :c]

        # sync with the supervisor: the restart flow is fully done once this is answered
        current_children = supervisor.ask!(:children)
        expect(current_children.map(&:name)).to eq %w[a b c]

        # the child added before the failed one keeps its reference,
        # restarted children are replaced by new references at their original positions
        expect(current_children[0]).to eq original[:a]
        expect(current_children[1]).to eq siblings[:b]
        expect(current_children[2]).to eq siblings[:c]
        expect(current_children[1]).not_to eq original[:b]
        expect(current_children[2]).not_to eq original[:c]

        # old references are terminated, the untouched child is still alive
        expect(original[:a].ask!(:terminated?)).to be false
        expect(original[:b].ask!(:terminated?)).to be true
        expect(original[:c].ask!(:terminated?)).to be true

        # nothing else happened
        expect(log).to be_empty

        terminate_actors supervisor
      end

      it 'does not stack a second restart flow when a child fails during a restart' do
        log      = Queue.new
        siblings = {}
        controls = new_controls

        supervisor = build_supervisor(log, siblings, controls,
                                      max_restarts: 10, within_seconds: 60)
        3.times { log.pop } # boot

        # the restarted :b fails again right after its start,
        # the restarted :c blocks its start on a gate controlled by the test
        controls[:fail_after_start][:b] = true
        gate                            = Queue.new
        controls[:start_gate]           = gate

        siblings[:b] << :fail

        expect(log.pop).to eq [:stop, :c]
        expect(log.pop).to eq [:stop, :b]
        expect(log.pop[0..1]).to eq [:start, :b]
        expect(log.pop[0..1]).to eq [:start, :c] # :c is now blocked on the gate

        # :b failed again while the restart of :c was still in progress
        wait_until { siblings[:b].ask!(:paused?) }

        gate << :go # let :c finish starting

        # sync with the supervisor: the restart flow is fully done once this is answered
        supervisor.ask!(:children)

        # exactly one restart flow ran: no duplicated stop/start pairs were stacked
        expect(log).to be_empty

        terminate_actors supervisor
      end

      it 'escalates to the parent supervisor when the middle child keeps failing to start' do
        log        = Queue.new
        parent_log = Queue.new
        siblings   = {}
        controls   = new_controls
        now        = [100.0]
        clock      = -> { now[0] }

        parent, holder = spawn_supervised_tree(log, siblings, controls, parent_log,
                                               max_restarts: 3, within_seconds: 60, clock: clock)
        supervisor     = holder[:supervisor]

        expect(log.pop[0..1]).to eq [:start, :a]
        expect(log.pop[0..1]).to eq [:start, :b]
        expect(log.pop[0..1]).to eq [:start, :c]

        # the middle child keeps failing to (re)start
        controls[:start_failures][:b] = 10

        siblings[:b] << :fail

        expect(log.pop).to eq [:stop, :c]
        expect(log.pop).to eq [:stop, :b]
        # each failed start counts towards the limit, after 3 attempts the supervisor gives up
        3.times do
          expect(log.pop[0..1]).to eq [:start, :b]
          expect(log.pop).to eq [:stop, :b] # the failed start is torn down again
        end

        # the parent supervisor takes over instead of an infinite restart loop
        error = parent_log.pop
        expect(error).to be_a MaxRestartIntensityExceeded
        expect(error.supervisor.path).to eq '/parent/rest_for_one_supervisor'
        expect(error.max_restarts).to eq 3
        expect(error.within_seconds).to eq 60

        # no infinite loop: nothing else happens
        sleep 0.2
        expect(log).to be_empty
        expect(parent_log).to be_empty
        expect(supervisor.ask!(:paused?)).to be true

        terminate_actors parent
      end

      it 'escalates when restarts within the window exceed max_restarts' do
        log        = Queue.new
        parent_log = Queue.new
        siblings   = {}
        controls   = new_controls
        now        = [100.0]
        clock      = -> { now[0] }

        parent, holder = spawn_supervised_tree(log, siblings, controls, parent_log,
                                               max_restarts: 2, within_seconds: 10, clock: clock)
        supervisor     = holder[:supervisor]
        3.times { log.pop } # boot

        siblings[:b] << :fail # restart 1 at t=100
        4.times { log.pop }
        supervisor.ask!(:children) # sync

        now[0] = 105
        siblings[:b] << :fail # restart 2 at t=105
        4.times { log.pop }
        supervisor.ask!(:children) # sync

        now[0] = 106
        siblings[:b] << :fail # third restart within 10 seconds

        expect(parent_log.pop).to be_a MaxRestartIntensityExceeded
        expect(log).to be_empty # the failed child was left paused, no restart was started

        terminate_actors parent
      end

      it 'restarts again once the window has passed' do
        log        = Queue.new
        parent_log = Queue.new
        siblings   = {}
        controls   = new_controls
        now        = [100.0]
        clock      = -> { now[0] }

        parent, holder = spawn_supervised_tree(log, siblings, controls, parent_log,
                                               max_restarts: 2, within_seconds: 10, clock: clock)
        supervisor     = holder[:supervisor]
        3.times { log.pop } # boot

        siblings[:b] << :fail # restart 1 at t=100
        4.times { log.pop }
        supervisor.ask!(:children) # sync

        now[0] = 105
        siblings[:b] << :fail # restart 2 at t=105
        4.times { log.pop }
        supervisor.ask!(:children) # sync

        now[0] = 120 # the window has passed, the count starts over
        siblings[:b] << :fail
        expect(log.pop).to eq [:stop, :c]
        expect(log.pop).to eq [:stop, :b]
        expect(log.pop[0..1]).to eq [:start, :b]
        expect(log.pop[0..1]).to eq [:start, :c]
        supervisor.ask!(:children) # sync

        sleep 0.2
        expect(parent_log).to be_empty # no escalation happened
        expect(log).to be_empty

        terminate_actors parent
      end

      it 'never restarts a temporary child and does not trigger a restart for it' do
        log      = Queue.new
        siblings = {}
        controls = new_controls

        supervisor = build_supervisor(log, siblings, controls,
                                      max_restarts: 10, within_seconds: 60,
                                      restart_policies: { b: :temporary })
        3.times { log.pop } # boot

        siblings[:b] << :fail

        # the temporary child is left paused, nobody is restarted
        wait_until { siblings[:b].ask!(:paused?) }
        sleep 0.1
        expect(log).to be_empty
        expect(siblings[:a].ask!(:ping)).to eq :ping
        expect(siblings[:c].ask!(:ping)).to eq :ping

        terminate_actors supervisor
      end

      it 'stops but does not replace temporary successors, transient and permanent ones are replaced' do
        log      = Queue.new
        siblings = {}
        controls = new_controls

        supervisor = build_supervisor(log, siblings, controls,
                                      max_restarts: 10, within_seconds: 60,
                                      restart_policies: { b: :temporary, c: :transient })
        3.times { log.pop } # boot

        # the first child fails, all children are its successors
        siblings[:a] << :fail

        expect(log.pop).to eq [:stop, :c]
        expect(log.pop).to eq [:stop, :b]
        expect(log.pop).to eq [:stop, :a]
        # :b is temporary so it is not replaced, :a (permanent) and :c (transient) are
        expect(log.pop[0..1]).to eq [:start, :a]
        expect(log.pop[0..1]).to eq [:start, :c]

        current_children = supervisor.ask!(:children) # sync
        expect(current_children.map(&:name)).to eq %w[a c]

        expect(log).to be_empty

        terminate_actors supervisor
      end
    end
  end
end
