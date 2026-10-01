require 'concurrent/context'

module Concurrent
  RSpec.describe ContextVar do
    it 'returns the default when nothing was set' do
      v = described_class.new(:v, 42)
      expect(v.value).to eq 42
    end

    it 'supports a default block evaluated on the reading thread' do
      v = described_class.new(:v) { Thread.current.object_id }
      main_value = v.value
      other_value = Thread.new { v.value }.value
      expect(other_value).not_to eq main_value
    end

    it 'does not allow a default value and a default block together' do
      expect { described_class.new(:v, 1) { 2 } }.to raise_error(ArgumentError)
    end

    it 'is thread local' do
      v = described_class.new(:v)
      v.value = 'main'
      Thread.new do
        expect(v.value).to be_nil
        v.value = 'worker'
        expect(v.value).to eq 'worker'
      end.join
      expect(v.value).to eq 'main'
    end

    it 'distinguishes an explicitly set nil' do
      v = described_class.new(:v, 'default')
      v.value = nil
      expect(v.value).to be_nil
      snapshot = Context.capture
      expect(snapshot).not_to be_empty
    end

    describe '#bind' do
      it 'binds for the duration of the block and restores the old value' do
        v = described_class.new(:v, 'old')
        v.bind('new') do
          expect(v.value).to eq 'new'
        end
        expect(v.value).to eq 'old'
      end

      it 'restores even when the block raises' do
        v = described_class.new(:v, 'old')
        expect { v.bind('new') { raise 'boom' } }.to raise_error('boom')
        expect(v.value).to eq 'old'
      end

      it 'requires a block' do
        v = described_class.new(:v)
        expect { v.bind('x') }.to raise_error(ArgumentError)
      end
    end

    describe 'copy configuration' do
      it 'shares the reference by default' do
        v = described_class.new(:v)
        value = ['a']
        v.value = value
        expect(Context.capture[v]).to equal value
      end

      it 'dups the value on capture with dup_on_enter' do
        v = described_class.new(:v, nil, dup_on_enter: true)
        value = ['a']
        v.value = value
        captured = Context.capture[v]
        expect(captured).to eq ['a']
        expect(captured).not_to equal value
      end

      it 'uses the copy_on_enter proc, taking precedence over dup' do
        v = described_class.new(:v, nil,
                                dup_on_enter: true,
                                copy_on_enter: ->(value) { value + ['b'] })
        v.value = ['a']
        expect(Context.capture[v]).to eq ['a', 'b']
      end
    end
  end

  RSpec.describe Context do
    let(:var) { ContextVar.new(:"var_#{object_id}") }
    let(:other) { ContextVar.new(:"other_#{object_id}") }

    after do
      # never leak test bindings onto the RSpec thread
      [var, other].each { |v| v.__set_raw_value__(Context::UNSET) }
    end

    describe '.capture' do
      it 'contains only registered variables which are set on the current thread' do
        var.value = 1
        snapshot = described_class.capture
        expect(snapshot[var]).to eq 1
        expect(snapshot[other]).to be_nil
      end

      it 'copies values according to the variable configuration' do
        v = ContextVar.new(:"dup_#{object_id}", nil, dup_on_enter: true)
        v.value = ['x']
        expect(described_class.capture[v]).to eq ['x']
        v.__set_raw_value__(Context::UNSET)
      end
    end

    describe '.with_snapshot' do
      it 'installs the snapshot and restores previous bindings afterwards' do
        var.value = 'before'
        other.value = 'other-before'
        snapshot = described_class.capture

        var.value = 'changed'
        described_class.with_snapshot(snapshot) do
          expect(var.value).to eq 'before'
          expect(other.value).to eq 'other-before'
        end
        expect(var.value).to eq 'changed'
        expect(other.value).to eq 'other-before'
      end

      it 'restores bindings even when the block raises' do
        var.value = 'before'
        expect do
          described_class.with_snapshot(described_class.capture) do
            var.value = 'inside'
            raise 'boom'
          end
        end.to raise_error('boom')
        expect(var.value).to eq 'before'
      end

      it 'restores the exact previous object even when the var is copied on capture' do
        v = ContextVar.new(:"identity_#{object_id}", nil, dup_on_enter: true)
        original = ['x']
        v.value = original
        described_class.with_snapshot(described_class.capture) do
          v.value << 'mutated-in-task'
        end
        expect(v.value).to equal original
        expect(v.value).to eq ['x']
      end

      it 'leaves no residual value for a variable that was unset beforehand' do
        # var was never set on this thread
        described_class.with_snapshot(described_class.capture) do
          var.value = 'set-inside'
        end
        expect(var.__raw_value__).to eq Context::UNSET
      end

      it 'unsets a variable created and set inside the block' do
        created = nil
        described_class.with_snapshot(described_class.capture) do
          created = ContextVar.new(:"created_#{object_id}")
          created.value = 'inside'
        end
        expect(created.__raw_value__).to eq Context::UNSET
      end

      it 'captures the current context when no snapshot is given' do
        var.value = 'now'
        described_class.with_snapshot do
          expect(var.value).to eq 'now'
        end
      end
    end
  end
end
