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

    # Signals that a supervisor restarted its children too many times within
    # the configured time window. The supervisor pauses itself with this error
    # so its own parent supervisor can take over, instead of restarting forever.
    class MaxRestartIntensityExceeded < Error
      include TypeCheck

      # @return [Reference] the supervisor which exceeded its restart intensity
      attr_reader :supervisor
      # @return [Integer] max number of restarts allowed within {#within_seconds}
      attr_reader :max_restarts
      # @return [Numeric] the time window in seconds
      attr_reader :within_seconds

      def initialize(supervisor, max_restarts, within_seconds)
        @supervisor     = Type! supervisor, Reference
        @max_restarts   = max_restarts
        @within_seconds = within_seconds
        super format('%s exceeded max restart intensity: %d restarts within %s seconds',
                     supervisor.path, max_restarts, within_seconds)
      end
    end
  end
end
