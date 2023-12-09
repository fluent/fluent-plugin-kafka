# This is required for `rdkafka` version >= 0.12.0
# Overriding the close method in order to provide a time limit for when it should be forcibly closed
class Rdkafka::Producer::Client
  # return false if producer is forcefully closed, otherwise return true
  def close(timeout=nil)
    return unless @native

    # Indicate to polling thread that we're closing
    @polling_thread[:closing] = true
    # Wait for the polling thread to finish up
    thread = @polling_thread.join(timeout)

    Rdkafka::Bindings.rd_kafka_destroy(@native)

    @native = nil

    return !thread.nil?
  end
end

class Rdkafka::Producer
  def close(timeout = nil)
    ObjectSpace.undefine_finalizer(self)

    return @client.close(timeout)
  end
end
