require 'concurrent/edge/task_scope'

RSpec.describe 'Concurrent' do
  describe 'TaskScope', edge: true do

    it 'runs spawned tasks in parallel and collects their results in spawn order' do
      scope = Concurrent::TaskScope.open do |s|
        s.spawn { 1 }
        s.spawn(2) { |_token, v| v * 10 }
        s.spawn { sleep 0.05; 3 }
      end

      expect(scope).to be_complete
      expect(scope.results).to eq [[true, 1, nil], [true, 20, nil], [true, 3, nil]]
      expect(scope.tasks.map(&:value)).to eq [1, 20, 3]
    end

    it 'does not return from the block until all admitted tasks have finished' do
      finished = Concurrent::AtomicFixnum.new(0)
      scope    = Concurrent::TaskScope.open do |s|
        3.times { s.spawn { sleep 0.05; finished.increment } }
      end

      expect(finished.value).to eq 3
      expect(scope.tasks).to all(be_done)
    end

    it 'cancels the remaining tasks cooperatively when one task fails' do
      observed_cancellation = Concurrent::AtomicBoolean.new(false)

      expect do
        Concurrent::TaskScope.open do |s|
          s.spawn { sleep 0.01; raise 'boom' }
          s.spawn do |token|
            loop do
              if token.canceled?
                observed_cancellation.make_true
                token.check!
              end
              Thread.pass
            end
          end
        end
      end.to raise_error(Concurrent::TaskScope::TaskGroupError) { |error|
        expect(error.failures.size).to eq 1
        expect(error.failures.first.message).to eq 'boom'
      }

      expect(observed_cancellation.true?).to be_truthy
    end

    it 'preserves the individual reasons of multiple failures' do
      error_one = RuntimeError.new('first reason')
      error_two = ArgumentError.new('second reason')

      expect do
        Concurrent::TaskScope.open do |s|
          s.spawn { sleep 0.01; raise error_one }
          s.spawn { sleep 0.02; raise error_two }
        end
      end.to raise_error(Concurrent::TaskScope::TaskGroupError) { |error|
        expect(error.failures).to match_array([error_one, error_two])
        expect(error.message).to include('first reason', 'second reason')
      }
    end

    it 'cancels and collects tasks before re-raising an error raised by the block' do
      task_started  = Queue.new
      task_finished = Concurrent::AtomicBoolean.new(false)

      expect do
        Concurrent::TaskScope.open do |s|
          s.spawn do |token|
            task_started << true
            loop { token.check!; Thread.pass }
          rescue Concurrent::CancelledOperationError
            task_finished.make_true
            raise
          end
          task_started.pop
          raise 'block failed'
        end
      end.to raise_error(RuntimeError, 'block failed')

      expect(task_finished.true?).to be_truthy
    end

    it 'attaches the block error as cause when both the block and tasks fail' do
      expect do
        Concurrent::TaskScope.open do |s|
          s.spawn { sleep 0.01; raise 'child boom' }
          sleep 0.1
          raise 'block boom'
        end
      end.to raise_error(Concurrent::TaskScope::TaskGroupError) { |error|
        expect(error.failures.map(&:message)).to eq ['child boom']
        expect(error.cause).to be_a(RuntimeError)
        expect(error.cause.message).to eq 'block boom'
      }
    end

    it 'raises LifecycleError when spawning into a closed scope' do      captured = nil
      Concurrent::TaskScope.open { |s| captured = s }
      expect { captured.spawn { 1 } }.to raise_error(Concurrent::LifecycleError)
    end

    it 'supports manual cooperative cancellation of its own tasks only' do
      task_started = Queue.new
      scope        = Concurrent::TaskScope.open do |s|
        s.spawn do |token|
          task_started << true
          loop { token.check!; Thread.pass }
        end
        task_started.pop
        s.cancel!
      end

      expect(scope.cancellation.canceled?).to be_truthy
      expect(scope.tasks.first.cancelled?).to be_truthy
      expect(scope.failures).to be_empty
    end

    context 'with nested scopes' do
      it 'manages only its own tasks and never shuts down the shared executor' do
        inner_completed = Concurrent::AtomicBoolean.new(false)

        Concurrent::TaskScope.open do |outer|
          outer.spawn do |token|
            Concurrent::TaskScope.open(cancellation: token) do |inner|
              inner.spawn { sleep 0.02; :inner }
            end
            inner_completed.make_true
          end
          outer.spawn { sleep 0.01; :outer }
        end

        expect(inner_completed.true?).to be_truthy
        # the shared pool is still usable afterwards
        expect(Concurrent.global_io_executor.running?).to be_truthy
        probe = Concurrent::TaskScope.open { |s| s.spawn { :still_working } }
        expect(probe.tasks.first.value).to eq :still_working
      end

      it 'propagates outer cancellation into the nested scope cooperatively' do
        inner_observed = Concurrent::AtomicBoolean.new(false)

        expect do
          Concurrent::TaskScope.open do |outer|
            outer.spawn do |token|
              Concurrent::TaskScope.open(cancellation: token) do |inner|
                inner.spawn do |inner_token|
                  loop do
                    if inner_token.canceled?
                      inner_observed.make_true
                      inner_token.check!
                    end
                    Thread.pass
                  end
                end
              end
            rescue Concurrent::TaskScope::TaskGroupError
              # the inner scope itself only saw cooperative cancellation
            end
            outer.spawn { sleep 0.01; raise 'outer failure' }
          end
        end.to raise_error(Concurrent::TaskScope::TaskGroupError)

        expect(inner_observed.true?).to be_truthy
      end
    end

    context 'with a task which ignores cancellation until an explicit checkpoint' do
      it 'does not report the scope complete until the task reaches its checkpoint' do
        may_proceed   = Queue.new
        at_checkpoint = Queue.new
        scope_done    = Concurrent::AtomicBoolean.new(false)
        captured      = nil
        raised        = nil

        thread = Thread.new do
          begin
            Concurrent::TaskScope.open do |s|
              captured = s
              s.spawn { sleep 0.01; raise 'boom' }
              s.spawn do |token|
                # ignores the cancellation until it is allowed to continue
                may_proceed.pop
                at_checkpoint << true
                token.check!
              end
            end
          rescue => error
            raised = error
          end
          scope_done.make_true
        end

        sleep 0.2
        # the failing task already triggered cancellation, but the scope must
        # not report itself finished while a task still ignores it
        expect(captured.complete?).to be_falsey
        expect(scope_done.false?).to be_truthy

        may_proceed << :go
        at_checkpoint.pop
        sleep 0.1
        # the checkpoint was reached but the scope still waits for the task to finish
        thread.join(5)
        expect(thread).not_to be_alive
        expect(scope_done.true?).to be_truthy
        expect(captured.complete?).to be_truthy
        expect(raised).to be_a(Concurrent::TaskScope::TaskGroupError)
        expect(captured.tasks.last.cancelled?).to be_truthy
      end
    end

    describe 'Task' do
      it 'exposes done/success/failed state and result triplet' do
        scope = Concurrent::TaskScope.open do |s|
          @ok = s.spawn { 42 }
        end
        expect(@ok).to be_done
        expect(@ok).to be_success
        expect(@ok).not_to be_failed
        expect(@ok.result).to eq [true, 42, nil]
        expect(@ok.wait).to eq @ok
        expect(scope.tasks).to eq [@ok]
      end

      it 'raises the reason from #value when the task failed' do
        task = nil
        expect do
          Concurrent::TaskScope.open do |s|
            task = s.spawn { raise ArgumentError, 'bad' }
          end
        end.to raise_error(Concurrent::TaskScope::TaskGroupError)

        expect(task).to be_failed
        expect(task.reason).to be_a(ArgumentError)
        expect { task.value }.to raise_error(ArgumentError, 'bad')
        expect(task.result).to eq [false, nil, task.reason]
      end
    end
  end
end
