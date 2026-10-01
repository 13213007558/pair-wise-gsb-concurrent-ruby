require 'concurrent/task_scope'

RSpec.describe 'Concurrent' do
  describe 'TaskScope' do

    specify 'spawns tasks, waits for their results and returns the block value' do
      result = Concurrent::TaskScope.run do |scope|
        a = scope.spawn { 1 + 1 }
        b = scope.spawn { |token| token.check!; 40 + 2 }

        expect(scope.wait(a)).to eq 2
        expect(scope.wait(b)).to eq 42
        expect(scope.complete?).to be true
        :block_value
      end

      expect(result).to eq :block_value
    end

    specify 'passes spawn arguments to the task after the token' do
      Concurrent::TaskScope.run do |scope|
        future = scope.spawn(1, 2) { |_token, a, b| a + b }
        expect(scope.wait(future)).to eq 3
      end
    end

    specify 'a failed task asks its siblings to stop cooperatively' do
      sibling_state = nil

      expect do
        Concurrent::TaskScope.run do |scope|
          scope.spawn { |_token| sleep 0.05; raise 'boom' }
          scope.spawn do |token|
            begin
              loop { token.check!; Thread.pass }
            rescue Concurrent::CancelledOperationError
              sibling_state = :cancelled
            end
          end
        end
      end.to raise_error(Concurrent::TaskScope::Failure)

      expect(sibling_state).to eq :cancelled
    end

    specify 'collects the results of admitted tasks, preserving each failure reason' do
      error_one = RuntimeError.new 'first reason'
      error_two = ArgumentError.new 'second reason'

      failure = nil
      begin
        Concurrent::TaskScope.run do |scope|
          scope.spawn { |_token| sleep 0.02; raise error_one }
          scope.spawn { |_token| sleep 0.05; raise error_two }
          scope.spawn { 42 }
        end
      rescue Concurrent::TaskScope::Failure => failure_
        failure = failure_
      end

      expect(failure).not_to be_nil
      expect(failure.errors).to match_array [error_one, error_two]
      expect(failure.message).to include('first reason', 'second reason')
    end

    specify 're-raises a block error after asking the tasks to stop and collecting them' do
      cancelled = nil

      expect do
        Concurrent::TaskScope.run do |scope|
          scope.spawn do |token|
            begin
              loop { token.check!; Thread.pass }
            rescue Concurrent::CancelledOperationError
              cancelled = true
            end
          end
          sleep 0.02
          raise 'block failed'
        end
      end.to raise_error('block failed')

      expect(cancelled).to eq true
    end

    specify 'a nested scope manages only its own tasks and does not stop the shared pool' do
      inner_size          = nil
      outer_size          = nil
      outer_cancelled_mid = nil

      Concurrent::TaskScope.run do |outer|
        nested = outer.spawn do |_token|
          Concurrent::TaskScope.run do |inner|
            inner.spawn { 1 }
            inner.spawn { 2 }
            result              = inner.wait(inner.spawn { 3 })
            inner_size          = inner.size
            outer_cancelled_mid = outer.cancellation.canceled?
            result
          end
        end
        outer.spawn { |token| sleep 0.05; token.check!; :sibling_done }
        outer_size = outer.size

        expect(outer.wait(nested)).to eq 3
        # the inner scope tracks only its own tasks, the outer only its own
        expect(inner_size).to eq 3
        expect(outer_size).to eq 2
        # the inner scope finishing does not cancel the outer scope
        expect(outer_cancelled_mid).to be false
      end

      # the shared executor pool is still usable afterwards
      expect(Concurrent::Promises.future_on(:io) { :pool_alive }.value!).to eq :pool_alive
    end

    specify 'wait rejects tasks of another scope' do
      other = Concurrent::TaskScope.new
      Concurrent::TaskScope.run do |scope|
        task = scope.spawn { 1 }
        expect { other.wait(task) }.to raise_error(ArgumentError)
        expect(scope.wait(task)).to eq 1
      end
    end

    specify 'cannot spawn into a closed scope or run a scope twice' do
      scope = Concurrent::TaskScope.new
      scope.run { |s| s.spawn { 1 } }

      expect(scope.open?).to be false
      expect { scope.spawn { 2 } }.to raise_error(Concurrent::Error)
      expect { scope.run { } }.to raise_error(Concurrent::Error)
    end

    specify 'a task ignoring cancellation until its checkpoint keeps the scope incomplete' do
      scope           = Concurrent::TaskScope.new
      stubborn_done   = false
      complete_midway = nil
      started         = Concurrent.monotonic_time

      failure = nil
      begin
        scope.run do |s|
          # fails quickly, cancelling the scope's token
          failing = s.spawn { |_token| sleep 0.02; raise 'boom' }
          # ignores the cancellation until it reaches its checkpoint
          s.spawn do |token|
            sleep 0.4 # busy work, deliberately not checking the token
            token.check! rescue nil # the explicit checkpoint
            stubborn_done = true
          end

          failing.wait # wait until the failure happened and cancelled the token
          sleep 0.05
          # the token is cancelled, but the stubborn task is still running,
          # so the scope must not report itself complete
          complete_midway = s.complete?
        end
      rescue Concurrent::TaskScope::Failure => failure_
        failure = failure_
      end

      expect(complete_midway).to be false
      expect(failure).not_to be_nil
      expect(failure.errors.size).to eq 1
      # the scope waited for the stubborn task before exiting
      expect(stubborn_done).to be true
      expect(Concurrent.monotonic_time - started).to be >= 0.4
      expect(scope.complete?).to be true
    end
  end
end
