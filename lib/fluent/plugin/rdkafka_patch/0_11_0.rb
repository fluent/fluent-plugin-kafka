module Fluent
  module RdkafkaProducerShutdown
    def initialize(*args)
      @produce_mutex = Mutex.new
      super
    end

    def produce(**kwargs)
      @produce_mutex.synchronize do
        if @closing
          raise Rdkafka::ClosedProducerError.new(__method__) if defined?(Rdkafka::ClosedProducerError)
          raise "Producer is closing"
        end

        super
      end
    end

    # Return false on timeout without destroying the handle, otherwise return true.
    def close(timeout = nil)
      @produce_mutex.synchronize { @closing = true }

      # Delivery callbacks may call produce, so do not hold the mutex while joining.
      return false unless @polling_thread.join(timeout)

      super()
      true
    end
  end
end

Rdkafka::Producer.prepend(Fluent::RdkafkaProducerShutdown)
