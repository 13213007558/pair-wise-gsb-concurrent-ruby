require 'concurrent/future'
require 'concurrent/promise'
require 'concurrent/executor/single_thread_executor'

module Concurrent
  RSpec.describe 'context propagation with Future and Promise' do
    let(:trace_id) { ContextVar.new(:"trace_#{object_id}") }

    after { trace_id.__set_raw_value__(Context::UNSET) }

    describe Concurrent::Future do
      it 'does not propagate context by default' do
        trace_id.value = 'abc'
        future = Concurrent::Future.execute(executor: Concurrent.global_io_executor) do
          trace_id.value
        end
        expect(future.wait!.value).to be_nil
      end

      it 'propagates registered context when propagate_context: true' do
        trace_id.value = 'abc'
        future = Concurrent::Future.execute(
          executor: Concurrent.global_io_executor,
          propagate_context: true
        ) { trace_id.value }
        expect(future.wait!.value).to eq 'abc'
      end

      it 'captures at execute time and restores the worker afterwards' do
        single = Concurrent::SingleThreadExecutor.new
        trace_id.value = 'first'
        first_done = Concurrent::CountDownLatch.new(1)
        release = Concurrent::CountDownLatch.new(1)
        first = Concurrent::Future.new(executor: single, propagate_context: true) do
          first_done.count_down
          release.wait
          trace_id.value
        end
        first.execute
        first_done.wait

        trace_id.value = 'second'
        second = Concurrent::Future.execute(
          executor: single, propagate_context: true
        ) { trace_id.value }
        release.count_down

        expect(first.wait!.value).to eq 'first'
        expect(second.wait!.value).to eq 'second'
        expect(trace_id.value).to eq 'second'
        single.shutdown
        single.wait_for_termination
      end
    end

    describe Concurrent::Promise do
      it 'propagates context to the root body when propagate_context: true' do
        trace_id.value = 'root'
        promise = Concurrent::Promise.execute(propagate_context: true) do
          trace_id.value
        end
        expect(promise.wait.value!).to eq 'root'
      end

      it 'runs a continuation with the context captured at registration' do
        trace_id.value = 'root-ctx'
        root = Concurrent::Promise.execute(propagate_context: true) { |r| r }

        trace_id.value = 'registration-ctx'
        child = root.then(propagate_context: true) { trace_id.value }
        trace_id.value = 'later-ctx'

        expect(child.wait.value!).to eq 'registration-ctx'
      end

      it 'uses the registration context for the rejection handler' do
        error = RuntimeError.new('nope')
        root = Concurrent::Promise.execute(propagate_context: true) { raise error }

        trace_id.value = 'rescue-ctx'
        handled = root.then(->(_reason) { trace_id.value }, propagate_context: true)
        trace_id.value = nil

        expect(handled.wait.value!).to eq 'rescue-ctx'
      end

      it 'inherits the propagation flag from the parent promise' do
        trace_id.value = 'chain'
        root = Concurrent::Promise.execute(propagate_context: true) { 1 }
        child = root.then { |v| v + 1 }
        grandchild = child.then { trace_id.value }
        trace_id.value = nil
        expect(grandchild.wait.value!).to eq 'chain'
      end

      it 'does not propagate when the flag is off, even if the root has it on' do
        trace_id.value = 'root'
        root = Concurrent::Promise.execute(propagate_context: true) { 1 }
        child = root.then(propagate_context: false) { trace_id.value }
        trace_id.value = nil
        expect(child.wait.value!).to be_nil
      end
    end
  end
end
