require 'concurrent/actor'
require 'thread'

module Concurrent
  module Actor
    class RecordingDependent < RestartingContext
      def self.reset_state
        @state = { mutex: Mutex.new, attempts: Hash.new(0), fail_always: false }
      end

      def self.state
        @state ||= reset_state
      end

      def initialize(id, events)
        @id     = id
        @events = events
        self.class.state[:mutex].synchronize do
          self.class.state[:attempts][@id] += 1
          raise 'b cannot start' if @id == :b && self.class.state[:fail_always]
        end
        @events << [:start, @id, object_id]
      end

      def on_message(message)
        case message
        when :fail_once
          raise 'b failed'
        when :fail_always
          self.class.state[:fail_always] = true
          raise 'b failed'
        else
          pass
        end
      end

      def on_event(event)
        @events << [:stop, @id, object_id] if event.is_a?(::Array) && event.first == :terminated
      end
    end

    class CrashableChild < RestartingContext
      def on_message(message)
        raise 'child failed' if message == :boom
      end
    end

    RSpec.describe Supervisor, edge: true do
      let(:events) { [] }

      after do
        [@supervisor, @outer].each do |actor|
          actor.ask(:terminate!).wait if actor && !actor.ask!(:terminated?)
        end
      end

      def child_spec(id, restart: :permanent)
        {
          id:      id,
          class:   RecordingDependent,
          args:    [id, events],
          restart: restart
        }
      end

      def wait_until(timeout = 5)
        deadline = Time.now + timeout
        sleep 0.01 until yield || Time.now >= deadline
        expect(yield).to be_truthy
      end

      it 'stops the failed child and successors in reverse, then restarts in order' do
        @supervisor = described_class.spawn!(
          :dependencies,
          [child_spec(:a), child_spec(:b), child_spec(:c)],
          strategy: :rest_for_one
        )
        old_b = @supervisor.ask!([:child, :b])
        old_c = @supervisor.ask!([:child, :c])

        old_b << :fail_once
        wait_until { @supervisor.ask!([:child, :b]) != old_b }

        expect(events).to match [
          [:start, :a, an_instance_of(Integer)],
          [:start, :b, an_instance_of(Integer)],
          [:start, :c, an_instance_of(Integer)],
          [:stop, :c, an_instance_of(Integer)],
          [:stop, :b, an_instance_of(Integer)],
          [:start, :b, an_instance_of(Integer)],
          [:start, :c, an_instance_of(Integer)]
        ]
        expect(@supervisor.ask!([:child, :b])).not_to eq old_b
        expect(@supervisor.ask!([:child, :c])).not_to eq old_c
      end

      it 'restarts from the failed dependency when startup repeatedly fails and lets the parent take over' do
        RecordingDependent.reset_state
        specs = -> { [child_spec(:a), child_spec(:b), child_spec(:c)] }
        supervisor_class = described_class
        @outer = supervisor_class.spawn! :outer, [
          {
            id: :inner,
            class: supervisor_class,
            args: ->(_) do
              RecordingDependent.state[:fail_always] = false
              [specs.call, strategy: :rest_for_one, max_restarts: 1, max_seconds: 10]
            end
          }
        ], strategy: :one_for_one

        inner = @outer.ask!([:child, :inner])
        inner.ask!(supervisor_class::READY)
        inner.ask([:child, :b]).value! << :fail_always

        wait_until { inner.ask!(:terminated?) }
        wait_until { @outer.ask!([:child, :inner]) != inner }
        replacement = @outer.ask!([:child, :inner])
        replacement.ask!(supervisor_class::READY)

        expect(events).to start_with(
          [:start, :a, an_instance_of(Integer)],
          [:start, :b, an_instance_of(Integer)],
          [:start, :c, an_instance_of(Integer)],
          [:stop, :c, an_instance_of(Integer)],
          [:stop, :b, an_instance_of(Integer)]
        )
        expect(events).to include([:stop, :a, an_instance_of(Integer)])
        expect(events.last(3)).to match [
          [:start, :a, an_instance_of(Integer)],
          [:start, :b, an_instance_of(Integer)],
          [:start, :c, an_instance_of(Integer)]
        ]
        expect(@outer.ask!(:terminated?)).to be_falsey
      end

      it 'uses temporary, transient, and permanent restart rules' do
        child_factory = proc do |id|
          ready = Promises.resolvable_future
          child = CrashableChild.spawn(name: id, initialized: ready)
          [child, ready]
        end

        @supervisor = described_class.spawn! :policies, [
          {id: :temporary, start: ->(_) { child_factory.call(:temporary) }, restart: :temporary},
          {id: :transient, start: ->(_) { child_factory.call(:transient) }, restart: :transient},
          {id: :permanent, start: ->(_) { child_factory.call(:permanent) }, restart: :permanent}
        ]
        temporary = @supervisor.ask!([:child, :temporary])
        transient = @supervisor.ask!([:child, :transient])
        permanent = @supervisor.ask!([:child, :permanent])

        temporary << :boom
        transient.ask(:terminate!)
        permanent << :boom

        wait_until { temporary.ask!(:terminated?) }
        wait_until { transient.ask!(:terminated?) }
        wait_until { @supervisor.ask!([:child, :permanent]) != permanent }

        expect(@supervisor.ask!([:child, :temporary])).to be_nil
        expect(@supervisor.ask!([:child, :transient])).to be_nil
        expect(@supervisor.ask!([:child, :permanent])).not_to eq permanent
      end

      it 'restarts every child with one_for_all' do
        @supervisor = described_class.spawn!(
          :all,
          [child_spec(:a), child_spec(:b), child_spec(:c)],
          strategy: :one_for_all
        )
        old_refs = %i[a b c].map { |id| @supervisor.ask!([:child, id]) }

        old_refs[1] << :fail_once
        wait_until { @supervisor.ask!([:child, :b]) != old_refs[1] }

        expect(events.last(6)).to match [
          [:stop, :c, an_instance_of(Integer)],
          [:stop, :b, an_instance_of(Integer)],
          [:stop, :a, an_instance_of(Integer)],
          [:start, :a, an_instance_of(Integer)],
          [:start, :b, an_instance_of(Integer)],
          [:start, :c, an_instance_of(Integer)]
        ]
      end

      it 'counts restarts with the injected clock window' do
        ticks = Class.new do
          attr_accessor :now
          def initialize; @now = 0; end
        end.new

        @supervisor = described_class.spawn! :timed, [
          {
            id: :child,
            start: ->(_) do
              ready = Promises.resolvable_future
              child = CrashableChild.spawn(name: :child, initialized: ready)
              [child, ready]
            end
          }
        ], max_restarts: 1, max_seconds: 10, clock: ticks

        old_child = @supervisor.ask!([:child, :child])
        old_child << :boom
        wait_until { @supervisor.ask!([:child, :child]) != old_child }
        wait_until { !@supervisor.ask!(:restarting?) }
        replacement = @supervisor.ask!([:child, :child])
        ticks.now = 5
        replacement << :boom
        wait_until { @supervisor.ask!(:terminated?) }
      end
    end
  end
end
