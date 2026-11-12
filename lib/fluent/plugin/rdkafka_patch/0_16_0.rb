class Rdkafka::NativeKafka
  # return false if producer is forcefully closed, otherwise return true
  def close(timeout=nil, object_id=nil)
    return true if closed?

    synchronize do
      # Indicate to the outside world that we are closing
      @closing = true

      thread_status = :unknown
      if @polling_thread
        # Indicate to polling thread that we're closing
        @polling_thread[:closing] = true

        # Wait for the polling thread to finish up,
        # this can be aborted in practice if this
        # code runs from a finalizer.
        thread_status = @polling_thread.join(timeout)

        # The polling thread leaves its loop only when the queue is empty, and it
        # must stop before @inner is destroyed. Purge the queue to stop it.
        unless thread_status
          code = Rdkafka::Bindings.rd_kafka_purge(@inner, Rdkafka::Bindings::RD_KAFKA_PURGE_F_QUEUE | Rdkafka::Bindings::RD_KAFKA_PURGE_F_INFLIGHT)
          raise Rdkafka::RdkafkaError.new(code) unless code.zero?
          @polling_thread.join
        end
      end

      # Destroy the client after locking both mutexes
      @poll_mutex.lock

      # This check prevents a race condition, where we would enter the close in two threads
      # and after unlocking the primary one that hold the lock but finished, ours would be unlocked
      # and would continue to run, trying to destroy inner twice
      return unless @inner

      Rdkafka::Bindings.rd_kafka_destroy(@inner)
      @inner = nil
      @opaque = nil

      !thread_status.nil?
    end
  end
end

class Rdkafka::Producer
  def close(timeout = nil)
    return true if closed?
    ObjectSpace.undefine_finalizer(self)

    closed = @native_kafka.close(timeout) do
      # We need to remove the topics references objects before we destroy the producer,
      # otherwise they would leak out
      @topics_refs_map.each_value do |refs|
        refs.each_value do |ref|
          Rdkafka::Bindings.rd_kafka_topic_destroy(ref)
        end
      end
    end

    @topics_refs_map.clear
    closed
  end
end
