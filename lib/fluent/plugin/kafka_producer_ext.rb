require 'kafka/producer'

module Kafka
  class Producer
    def produce2(value, key: nil, topic:, partition: nil, partition_key: nil)
      create_time = Time.now

      message = PendingMessage.new(
        value,
        key,
        topic,
        partition,
        partition_key,
        create_time,
        key.to_s.bytesize + value.to_s.bytesize
      )

      @target_topics.add(topic)
      @pending_message_queue.write(message)

      nil
    end
  end
end
