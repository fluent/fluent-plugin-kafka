require 'kafka/producer'

module Kafka
  class Producer
    def deliver_messages_ext(topics, msgs)
      @cluster.add_target_topics(topics)

      operation = ProduceOperation.new(
        cluster: @cluster,
        buffer: @buffer,
        required_acks: @required_acks,
        ack_timeout: @ack_timeout,
        compressor: @compressor,
        logger: @logger,
        instrumenter: @instrumenter,
      )
      attempt = 0

      loop do
        attempt += 1

        @cluster.refresh_metadata_if_necessary!

        msgs = assign_partitions_with(msgs)
        operation.execute

        if @required_acks.zero?
          # No response is returned by the brokers, so we can't know which messages
          # have been successfully written. Our only option is to assume that they all
          # have.
          @buffer.clear
        end

        if buffer_size.zero?
          break
        elsif attempt <= @max_retries
          @logger.warn "Failed to send all messages; attempting retry #{attempt} of #{@max_retries} after #{@retry_backoff}s"

          sleep @retry_backoff
        else
          @logger.error "Failed to send all messages; keeping remaining messages in buffer"
          break
        end
      end

      unless msgs.empty?
        return msgs
      end

      unless @buffer.empty?
        partitions = @buffer.map {|topic, partition, _| "#{topic}/#{partition}" }.join(", ")

        raise DeliveryFailed, "Failed to send messages to #{partitions}"
      end

      nil
    end

    def assign_partitions_with(msgs)
      failed_messages = []

      msgs.each do |message|
        partition = message.partition

        begin
          if partition.nil?
            partition_count = @cluster.partitions_for(message.topic).count
            partition = Partitioner.partition_for_key(partition_count, message)
          end

          @buffer.write(
            value: message.value,
            key: message.key,
            topic: message.topic,
            partition: partition,
            create_time: message.create_time,
          )
        rescue Kafka::Error => e
          failed_messages << message
        end
      end

      if failed_messages.any?
        @logger.error "Failed to assign partitions to #{failed_messages.count} messages"
        @cluster.mark_as_stale!
      end

      failed_messages
    end
  end
end
