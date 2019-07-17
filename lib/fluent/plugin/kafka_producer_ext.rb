require "set"
require "kafka/partitioner"
require "kafka/message_buffer"
require "kafka/produce_operation"
require "kafka/pending_message_queue"
require "kafka/pending_message"
require "kafka/compressor"
require 'kafka/producer'

# for out_kafka_buffered
module Kafka
  EMPTY_HEADER = {}

  class Producer
    def produce_for_buffered(value, key: nil, topic:, partition: nil, partition_key: nil, create_time: Time.now)
      message = PendingMessage.new(
        value: value,
        key: key,
        headers: EMPTY_HEADER,
        topic: topic,
        partition: partition,
        partition_key: partition_key,
        create_time: create_time
      )

      # If the producer is in transactional mode, all the message production
      # must be used when the producer is currently in transaction
      if @transaction_manager.transactional? && !@transaction_manager.in_transaction?
        raise 'You must trigger begin_transaction before producing messages'
      end

      @target_topics.add(topic)
      @pending_message_queue.write(message)

      nil
    end
  end
end

# for out_kafka2
module Kafka
  class Client
    def topic_producer(topic, compression_codec: nil, compression_threshold: 1, ack_timeout: 5, required_acks: :all, max_retries: 2, retry_backoff: 1, max_buffer_size: 1000, max_buffer_bytesize: 10_000_000, idempotent: false, transactional: false, transactional_id: nil, transactional_timeout: 60)
      cluster = initialize_cluster
      compressor = Compressor.new(
        codec_name: compression_codec,
        threshold: compression_threshold,
        instrumenter: @instrumenter,
      )

      transaction_manager = TransactionManager.new(
        cluster: cluster,
        logger: @logger,
        idempotent: idempotent,
        transactional: transactional,
        transactional_id: transactional_id,
        transactional_timeout: transactional_timeout,
      )

      TopicProducer.new(topic,
        cluster: cluster,
        transaction_manager: transaction_manager,
        logger: @logger,
        instrumenter: @instrumenter,
        compressor: compressor,
        ack_timeout: ack_timeout,
        required_acks: required_acks,
        max_retries: max_retries,
        retry_backoff: retry_backoff,
        max_buffer_size: max_buffer_size,
        max_buffer_bytesize: max_buffer_bytesize,
      )
    end
  end

  class TopicProducer
    def initialize(topic, cluster:, transaction_manager:, logger:, instrumenter:, compressor:, ack_timeout:, required_acks:, max_retries:, retry_backoff:, max_buffer_size:, max_buffer_bytesize:)
      @cluster = cluster
      @transaction_manager = transaction_manager
      @logger = logger
      @instrumenter = instrumenter
      @required_acks = required_acks == :all ? -1 : required_acks
      @ack_timeout = ack_timeout
      @max_retries = max_retries
      @retry_backoff = retry_backoff
      @max_buffer_size = max_buffer_size
      @max_buffer_bytesize = max_buffer_bytesize
      @compressor = compressor

      @topic = topic
      @cluster.add_target_topics(Set.new([topic]))

      # A buffer organized by topic/partition.
      @buffer = MessageBuffer.new

      # Messages added by `#produce` but not yet assigned a partition.
      @pending_message_queue = PendingMessageQueue.new
    end

    def produce(value, key: nil, partition: nil, partition_key: nil, headers: EMPTY_HEADER, create_time: Time.now)
      message = PendingMessage.new(
        value: value,
        key: key,
        headers: headers,
        topic: @topic,
        partition: partition,
        partition_key: partition_key,
        create_time: create_time
      )

      # If the producer is in transactional mode, all the message production
      # must be used when the producer is currently in transaction
      if @transaction_manager.transactional? && !@transaction_manager.in_transaction?
        raise 'You must trigger begin_transaction before producing messages'
      end

      @pending_message_queue.write(message)

      nil
    end

    def deliver_messages
      # There's no need to do anything if the buffer is empty.
      return if buffer_size == 0

      deliver_messages_with_retries
    end

    # Returns the number of messages currently held in the buffer.
    #
    # @return [Integer] buffer size.
    def buffer_size
      @pending_message_queue.size + @buffer.size
    end

    def buffer_bytesize
      @pending_message_queue.bytesize + @buffer.bytesize
    end

    # Deletes all buffered messages.
    #
    # @return [nil]
    def clear_buffer
      @buffer.clear
      @pending_message_queue.clear
    end

    # Closes all connections to the brokers.
    #
    # @return [nil]
    def shutdown
      @transaction_manager.close
      @cluster.disconnect
    end

    def init_transactions
      @transaction_manager.init_transactions
    end

    def begin_transaction
      @transaction_manager.begin_transaction
    end

    def commit_transaction
      @transaction_manager.commit_transaction
    end

    def abort_transaction
      @transaction_manager.abort_transaction
    end

    def transaction
      raise 'This method requires a block' unless block_given?
      begin_transaction
      yield
      commit_transaction
    rescue Kafka::Producer::AbortTransaction
      abort_transaction
    rescue
      abort_transaction
      raise
    end

    def deliver_messages_with_retries
      attempt = 0

      #@cluster.add_target_topics(@target_topics)

      operation = ProduceOperation.new(
        cluster: @cluster,
        transaction_manager: @transaction_manager,
        buffer: @buffer,
        required_acks: @required_acks,
        ack_timeout: @ack_timeout,
        compressor: @compressor,
        logger: @logger,
        instrumenter: @instrumenter,
      )

      loop do
        attempt += 1

        begin
          @cluster.refresh_metadata_if_necessary!
        rescue ConnectionError => e
          raise DeliveryFailed.new(e, buffer_messages)
        end

        assign_partitions!
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

      unless @pending_message_queue.empty?
        # Mark the cluster as stale in order to force a cluster metadata refresh.
        @cluster.mark_as_stale!
        raise DeliveryFailed.new("Failed to assign partitions to #{@pending_message_queue.size} messages", buffer_messages)
      end

      unless @buffer.empty?
        partitions = @buffer.map {|topic, partition, _| "#{topic}/#{partition}" }.join(", ")

        raise DeliveryFailed.new("Failed to send messages to #{partitions}", buffer_messages)
      end
    end

    def assign_partitions!
      failed_messages = []
      partition_count = @cluster.partitions_for(@topic).count

      @pending_message_queue.each do |message|
        partition = message.partition

        begin
          if partition.nil?
            partition = Partitioner.partition_for_key(partition_count, message)
          end

          @buffer.write(
            value: message.value,
            key: message.key,
            headers: message.headers,
            topic: message.topic,
            partition: partition,
            create_time: message.create_time,
          )
        rescue Kafka::Error => e
          failed_messages << message
        end
      end

      if failed_messages.any?
        failed_messages.group_by(&:topic).each do |topic, messages|
          @logger.error "Failed to assign partitions to #{messages.count} messages in #{topic}"
        end

        @cluster.mark_as_stale!
      end

      @pending_message_queue.replace(failed_messages)
    end

    def buffer_messages
      messages = []

      @pending_message_queue.each do |message|
        messages << message
      end

      @buffer.each do |topic, partition, messages_for_partition|
        messages_for_partition.each do |message|
          messages << PendingMessage.new(
            value: message.value,
            key: message.key,
            headers: message.headers,
            topic: topic,
            partition: partition,
            partition_key: nil,
            create_time: message.create_time
          )
        end
      end

      messages
    end
  end
end
