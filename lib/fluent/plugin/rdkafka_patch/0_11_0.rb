class Rdkafka::Producer
  # return false if producer is forcefully closed, otherwise return true
  def close(timeout = nil)
    @closing = true
    # Wait for the polling thread to finish up
    # If the broker isn't alive, the thread doesn't exit
    if timeout
      thr = @polling_thread.join(timeout)
      return !!thr
    else
      @polling_thread.join
      return true
    end
  end
end
