require 'concurrent/actor/type_check'

module Concurrent
  module Actor
    Error = Class.new(StandardError)

    class ActorTerminated < Error
      include TypeCheck

      attr_reader :reference

      def initialize(reference)
        @reference = Type! reference, Reference
        super reference.path
      end
    end

    class UnknownMessage < Error
      include TypeCheck

      attr_reader :envelope

      def initialize(envelope)
        @envelope = Type! envelope, Envelope
        super "#{envelope.message.inspect} from #{envelope.sender_path}"
      end
    end

    class SupervisionError < Error
      attr_reader :supervisor

      def initialize(supervisor)
        unless supervisor.is_a?(Reference)
          raise ArgumentError, "expected a supervisor Reference, got #{supervisor.class}"
        end
        @supervisor = supervisor
        super "supervisor #{supervisor.path} exceeded its restart limit"
      end
    end
  end
end
