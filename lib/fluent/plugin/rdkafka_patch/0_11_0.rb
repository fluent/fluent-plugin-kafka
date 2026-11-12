class Rdkafka::Producer
  module CloseWithTimeout
    # return false if producer is forcefully closed, otherwise return true
    def close(timeout = nil)
      @closing = true
      # Wait for the polling thread to finish up
      # If the broker isn't alive, the thread doesn't exit
      return false unless @polling_thread.join(timeout)

      super()
      true
    end
  end

  prepend CloseWithTimeout
end
