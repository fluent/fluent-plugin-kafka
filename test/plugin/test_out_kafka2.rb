require 'helper'
require 'fluent/test/helpers'
require 'fluent/test/driver/input'
require 'fluent/test/driver/output'
require 'securerandom'

class Kafka2OutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  def base_config
    config_element('ROOT', '', {"@type" => "kafka2"}, [
                     config_element('format', "", {"@type" => "json"})
                   ])
  end

  def config(default_topic: "kitagawakeiko")
    base_config + config_element('ROOT', '', {"default_topic" => default_topic,
                                              "brokers" => "localhost:9092"}, [
                                 ])
  end

  def create_driver(conf = config, tag='test')
    Fluent::Test::Driver::Output.new(Fluent::Kafka2Output).configure(conf)
  end

  class DummyProducer
    attr_reader :produced

    # Coerces like ruby-kafka's Producer#produce, which is where an invalid
    # partition raises today
    def produce(value, key:, partition_key:, partition:, headers:, create_time:, topic:)
      @produced ||= []
      @produced << {value: value && value.to_s, key: key && key.to_s,
                    partition: partition && Integer(partition)}
    end

    def deliver_messages
    end

    def clear_buffer
    end
  end

  def create_chunk(records, tag: 'test', variables: nil)
    metadata = Fluent::Plugin::Buffer::Metadata.new(nil, tag, variables)
    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(metadata)
    chunk.extend(Fluent::ChunkMessagePackEventStreamer)
    chunk.append(records.map { |record| [event_time, record].to_msgpack })
    chunk
  end

  def test_configure
    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(base_config)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(config)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(config + config_element('buffer', "", {"@type" => "memory"}))
    }

    d = create_driver
    assert_equal 'kitagawakeiko', d.instance.default_topic
    assert_equal ['localhost:9092'], d.instance.brokers
  end

  data("cert without key" => {"ssl_client_cert" => "/path/to/cert.pem"},
       "key without cert" => {"ssl_client_cert_key" => "/path/to/key.pem"},
       "chain without cert" => {"ssl_client_cert_chain" => "/path/to/chain.pem"},
       "key password without key" => {"ssl_client_cert_key_password" => "secret"})
  def test_configure_incomplete_ssl_client_cert(params)
    conf = config + config_element('ROOT', '', params, [])

    assert_raise(Fluent::ConfigError) {
      create_driver(conf)
    }
  end

  def test_configure_ssl_client_cert_with_key
    conf = config + config_element('ROOT', '', {"ssl_client_cert" => "/path/to/cert.pem",
                                                "ssl_client_cert_key" => "/path/to/key.pem"}, [])

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(conf)
    }
  end

  data("sha256" => "sha256",
       "sha512" => "sha512")
  def test_configure_scram_mechanism(mechanism)
    conf = config + config_element('ROOT', '', {"username" => "testuser",
                                                "password" => "testpass",
                                                "scram_mechanism" => mechanism,
                                                "ssl_ca_certs_from_system" => "true"}, [])
    d = create_driver(conf)

    assert_equal mechanism, d.instance.scram_mechanism

    assert_nothing_raised {
      d.instance.refresh_client
    }
  end

  def test_configure_unsupported_scram_mechanism
    conf = config + config_element('ROOT', '', {"username" => "testuser",
                                                "password" => "testpass",
                                                "scram_mechanism" => "sha1"}, [])

    assert_raise(Fluent::ConfigError) {
      create_driver(conf)
    }
  end

  # Records the monitoring libraries instead of loading them, because they
  # need the activesupport, statsd-ruby and dogstatsd-ruby gems
  def stub_monitoring_require(d, missing: {})
    required = []
    original_require = d.instance.method(:require)
    stub(d.instance).require { |path|
      case path
      when *missing.keys
        raise LoadError, missing[path]
      when 'active_support', %r{\Akafka/}
        required << path
        true
      else
        original_require.call(path)
      end
    }
    required
  end

  def test_configure_monitoring_list
    d = Fluent::Test::Driver::Output.new(Fluent::Kafka2Output)
    required = stub_monitoring_require(d)
    d.configure(config + config_element('ROOT', '', {"monitoring_list" => "statsd,datadog"}, []))

    assert_equal ['statsd', 'datadog'], d.instance.monitoring_list
    assert_equal ['active_support', 'kafka/statsd', 'kafka/datadog'], required
  end

  data("misspelled library" => ["datdog", {"kafka/datdog" => "cannot load such file -- kafka/datdog"}, /'datdog'/],
       "missing gem" => ["statsd", {"kafka/statsd" => "cannot load such file -- statsd"}, /'statsd'/],
       "missing activesupport" => ["statsd", {"active_support" => "cannot load such file -- active_support"}, /activesupport/])
  def test_configure_missing_monitoring_library(data)
    entry, missing, message = data
    d = Fluent::Test::Driver::Output.new(Fluent::Kafka2Output)
    stub_monitoring_require(d, missing: missing)

    error = assert_raise(Fluent::ConfigError) {
      d.configure(config + config_element('ROOT', '', {"monitoring_list" => entry}, []))
    }
    assert_match message, error.message
    assert_match missing.values.first, error.message
  end

  data("non numeric partition" => "not-a-number",
       "hash partition" => {"x" => 1},
       "negative partition" => -2,
       "out of int32 range" => 2**31)
  def test_write_skips_event_with_invalid_partition(partition)
    d = create_driver
    producer = DummyProducer.new
    stub(d.instance).get_producer { producer }

    assert_nothing_raised {
      d.instance.write(create_chunk([{"a" => "b"}, {"a" => "c", "partition" => partition}, {"a" => "d"}]))
    }

    assert_equal ['{"a":"b"}', '{"a":"d"}'], producer.produced.collect { |message| message[:value] }
  end

  def test_write_keeps_event_with_zero_padded_partition
    d = create_driver
    producer = DummyProducer.new
    stub(d.instance).get_producer { producer }

    d.instance.write(create_chunk([{"a" => "b", "partition" => "010"}]))

    assert_equal [10], producer.produced.collect { |message| message[:partition] }
  end

  class KeyRecordingProducer < DummyProducer
    attr_reader :keys

    def produce(value, key:, partition_key:, partition:, headers:, create_time:, topic:)
      @keys ||= []
      @keys << [key, partition_key]
    end
  end

  data("integer" => [123, "123"],
       "float" => [1.5, "1.5"],
       "boolean" => [true, "true"],
       "string" => ["abc", "abc"])
  def test_write_converts_keys_to_string(data)
    value, expected = data
    d = create_driver
    producer = KeyRecordingProducer.new
    stub(d.instance).get_producer { producer }

    d.instance.write(create_chunk([
      {"message_key" => value, "partition_key" => value},
      {"message_key" => value},
      {"partition_key" => value},
    ]))

    assert_equal [[expected, expected], [expected, nil], [nil, expected]], producer.keys
  end

  def test_write_keeps_missing_keys_nil
    d = create_driver
    producer = KeyRecordingProducer.new
    stub(d.instance).get_producer { producer }

    d.instance.write(create_chunk([{"a" => "b"}]))

    assert_equal [[nil, nil]], producer.keys
  end

  class MessageRecordingProducer < DummyProducer
    def produce(value, key:, partition_key:, partition:, headers:, create_time:, topic:)
      @produced ||= []
      @produced << {record: JSON.parse(value), key: key, partition_key: partition_key, partition: partition}
    end
  end

  def nested_keys_config(params = {})
    config + config_element('ROOT', '', {"message_key_key" => "$.kafka.message_key",
                                         "partition_key_key" => "$.kafka.partition_key",
                                         "partition_key" => "$['kafka']['partition']"}.merge(params))
  end

  def write_records(conf, records)
    d = create_driver(conf)
    producer = MessageRecordingProducer.new
    stub(d.instance).get_producer { producer }
    d.instance.write(create_chunk(records))
    producer.produced
  end

  def test_write_with_nested_keys
    record = {"kafka" => {"message_key" => "mk", "partition_key" => "pk", "partition" => 3}, "a" => "b"}

    produced = write_records(nested_keys_config, [record])

    assert_equal [{record: record, key: "mk", partition_key: "pk", partition: 3}], produced
  end

  def test_write_excludes_nested_keys
    record = {"kafka" => {"message_key" => "mk", "partition_key" => "pk", "partition" => 3, "c" => "d"}, "a" => "b"}
    conf = nested_keys_config("exclude_message_key" => true, "exclude_partition_key" => true, "exclude_partition" => true)

    produced = write_records(conf, [record])

    assert_equal [{record: {"kafka" => {"c" => "d"}, "a" => "b"}, key: "mk", partition_key: "pk", partition: 3}], produced
  end

  data("missing parent" => {"a" => "b"},
       "missing field" => {"kafka" => {}, "a" => "b"},
       "parent is a String" => {"kafka" => "str", "a" => "b"},
       "parent is an Array" => {"kafka" => ["str"], "a" => "b"})
  def test_write_uses_defaults_for_nested_keys_not_found(record)
    defaults = {"default_message_key" => "dmk", "default_partition_key" => "dpk", "default_partition" => 1}

    [false, true].each do |exclude|
      conf = nested_keys_config(defaults.merge("exclude_message_key" => exclude, "exclude_partition_key" => exclude,
                                               "exclude_partition" => exclude))

      produced = write_records(conf, [record])

      assert_equal [{record: record, key: "dmk", partition_key: "dpk", partition: 1}], produced
    end
  end

  def test_write_keeps_dotted_key_names_as_top_level_fields
    conf = config + config_element('ROOT', '', {"message_key_key" => "kafka.message_key",
                                                "partition_key_key" => "kafka.partition_key",
                                                "partition_key" => "kafka.partition"})
    record = {"kafka.message_key" => "mk", "kafka.partition_key" => "pk", "kafka.partition" => 3,
              "kafka" => {"message_key" => "nested"}}

    produced = write_records(conf, [record])

    assert_equal [{record: record, key: "mk", partition_key: "pk", partition: 3}], produced
  end

  data("per thread producer" => false,
       "shared producer" => true)
  def test_write_failure_replaces_producer(share_producer)
    d = create_driver(base_config + config_element('ROOT', '', {"default_topic" => "kitagawakeiko",
                                                                "brokers" => "localhost:1",
                                                                "share_producer" => share_producer}))
    d.instance_start
    producer = d.instance.get_producer
    mock.proxy(producer).shutdown

    assert_raise(Kafka::ConnectionError) {
      d.instance.write(create_chunk([{"a" => "b"}]))
    }

    assert_not_same producer, d.instance.get_producer
  ensure
    d.instance_shutdown if d
  end

  data("per thread producer" => false,
       "shared producer" => true)
  def test_write_failure_keeps_error_when_producer_shutdown_fails(share_producer)
    d = create_driver(base_config + config_element('ROOT', '', {"default_topic" => "kitagawakeiko",
                                                                "brokers" => "localhost:1",
                                                                "share_producer" => share_producer}))
    d.instance_start
    producer = d.instance.get_producer
    stub(producer).shutdown { raise IOError }

    assert_raise(Kafka::ConnectionError) {
      d.instance.write(create_chunk([{"a" => "b"}]))
    }
  ensure
    d.instance_shutdown if d
  end

  # Fails like CustomProducer#deliver_messages while any buffered message is for an unknown topic
  class UnknownTopicProducer < DummyProducer
    attr_reader :delivered

    def initialize(unknown_topics)
      @unknown_topics = unknown_topics
      @buffer = []
      @delivered = []
    end

    def produce(value, key:, partition_key:, partition:, headers:, create_time:, topic:)
      @buffer << topic
    end

    def deliver_messages
      unless (@buffer & @unknown_topics).empty?
        raise Kafka::DeliveryFailed.new("Failed to assign partitions", @buffer.dup)
      end
      @delivered.concat(@buffer)
      @buffer.clear
    end

    def clear_buffer
      @buffer.clear
    end

    def pending_message_count
      @buffer.size
    end

    def unknown_topic?(topic)
      @unknown_topics.include?(topic)
    end
  end

  def unknown_topic_config(params = {})
    config(default_topic: "fallback") + config_element('ROOT', '', {"use_default_for_unknown_topic" => true}.merge(params))
  end

  def write_to_unknown_topic(conf, producer)
    d = create_driver(conf)
    stub(d.instance).get_producer { producer }
    stub(d.instance).refresh_client
    stub(d.instance).discard_producer
    d.instance.write(create_chunk([{"a" => "b"}, {"a" => "c"}], variables: {topic: "unknown"}))
    d
  end

  def test_write_retries_with_default_topic_for_unknown_topic
    producer = UnknownTopicProducer.new(["unknown"])

    d = write_to_unknown_topic(unknown_topic_config, producer)

    assert_equal ["fallback", "fallback"], producer.delivered
    assert_true d.logs.any? { |log| log.include?("'unknown' topic not found. Retry with 'fallback' topic") }
  end

  def test_write_retries_with_default_topic_before_discarding
    producer = UnknownTopicProducer.new(["unknown"])

    write_to_unknown_topic(unknown_topic_config("discard_kafka_delivery_failed" => true), producer)

    assert_equal ["fallback", "fallback"], producer.delivered
  end

  def test_write_raises_for_unknown_default_topic
    producer = UnknownTopicProducer.new(["unknown", "fallback"])

    assert_raise(Kafka::DeliveryFailed) {
      write_to_unknown_topic(unknown_topic_config, producer)
    }
    assert_equal [], producer.delivered
  end

  def test_write_discards_for_unknown_default_topic
    producer = UnknownTopicProducer.new(["unknown", "fallback"])

    assert_nothing_raised {
      write_to_unknown_topic(unknown_topic_config("discard_kafka_delivery_failed" => true), producer)
    }
    assert_equal [], producer.delivered
  end

  def test_write_raises_for_unknown_topic_without_use_default_for_unknown_topic
    producer = UnknownTopicProducer.new(["unknown"])

    assert_raise(Kafka::DeliveryFailed) {
      write_to_unknown_topic(config(default_topic: "fallback"), producer)
    }
    assert_equal [], producer.delivered
  end

  # Delivers the first buffered message, and fails for the others
  class PartlyDeliveringProducer < UnknownTopicProducer
    def deliver_messages
      @delivered << @buffer.shift
      raise Kafka::DeliveryFailed.new("Failed to assign partitions", @buffer.dup) unless @buffer.empty?
    end
  end

  def assert_write_keeps_topic(producer, discard)
    conf = unknown_topic_config("discard_kafka_delivery_failed" => discard)
    if discard
      assert_nothing_raised { write_to_unknown_topic(conf, producer) }
    else
      assert_raise(Kafka::DeliveryFailed) { write_to_unknown_topic(conf, producer) }
    end
  end

  data("raise" => false,
       "discard" => true)
  def test_write_keeps_topic_when_some_records_were_delivered(discard)
    producer = PartlyDeliveringProducer.new(["unknown"])

    assert_write_keeps_topic(producer, discard)

    assert_equal ["unknown"], producer.delivered
  end

  data("raise" => false,
       "discard" => true)
  def test_write_keeps_topic_when_topic_is_known_again(discard)
    producer = UnknownTopicProducer.new(["unknown"])
    stub(producer).unknown_topic?("unknown") { false }

    assert_write_keeps_topic(producer, discard)

    assert_equal [], producer.delivered
  end

  # Fails after sending the messages, for example when the acknowledgements are lost
  class SentMessagesProducer < UnknownTopicProducer
    def pending_message_count
      0
    end
  end

  data("raise" => false,
       "discard" => true)
  def test_write_keeps_topic_when_records_may_have_been_sent(discard)
    producer = SentMessagesProducer.new(["unknown"])

    assert_write_keeps_topic(producer, discard)

    assert_equal [], producer.delivered
  end

  data("Kafka, raise" => [Kafka::ConnectionError, false],
       "Kafka, discard" => [Kafka::ConnectionError, true],
       "socket, raise" => [Errno::ECONNRESET, false],
       "socket, discard" => [Errno::ECONNRESET, true])
  def test_write_keeps_topic_when_topic_check_fails(data)
    error_class, discard = data
    producer = UnknownTopicProducer.new(["unknown"])
    stub(producer).unknown_topic?("unknown") { raise error_class }

    assert_write_keeps_topic(producer, discard)

    assert_equal [], producer.delivered
  end

  class PartitionsForCluster
    def initialize(&partitions_for)
      @partitions_for = partitions_for
    end

    def add_target_topics(_topics)
    end

    def refresh_metadata_if_necessary!
    end

    def mark_as_stale!
    end

    def partitions_for(topic)
      @partitions_for.call(topic)
    end
  end

  def create_custom_producer(cluster)
    logger = Logger.new(IO::NULL)
    instrumenter = Kafka::Instrumenter.new(client_id: 'fluentd')
    Kafka::CustomProducer.new(
      cluster: cluster,
      transaction_manager: Kafka::TransactionManager.new(cluster: cluster, logger: logger),
      logger: logger,
      instrumenter: instrumenter,
      compressor: Kafka::Compressor.new(codec_name: nil, threshold: 1, instrumenter: instrumenter),
      ack_timeout: 5,
      required_acks: -1,
      max_retries: 0,
      retry_backoff: 0,
      max_buffer_size: 1000,
      max_buffer_bytesize: 10_000_000,
      partitioner: Kafka::Partitioner.new,
    )
  end

  def test_custom_producer_unknown_topic
    cluster = PartitionsForCluster.new { raise Kafka::UnknownTopicOrPartition }

    assert_true create_custom_producer(cluster).unknown_topic?("unknown")
  end

  def test_custom_producer_known_topic
    cluster = PartitionsForCluster.new { [] }

    assert_false create_custom_producer(cluster).unknown_topic?("known")
  end

  data("Kafka" => Kafka::ConnectionError,
       "TLS" => OpenSSL::SSL::SSLError,
       "DNS" => Resolv::ResolvError)
  def test_custom_producer_raises_failed_topic_check(error_class)
    cluster = PartitionsForCluster.new { raise error_class }

    assert_raise(error_class) { create_custom_producer(cluster).unknown_topic?("unknown") }
  end

  def test_custom_producer_keeps_messages_of_unknown_topic_pending
    cluster = PartitionsForCluster.new { raise Kafka::UnknownTopicOrPartition }
    producer = create_custom_producer(cluster)
    producer.produce("a", topic: "unknown")
    producer.produce("b", topic: "unknown")

    assert_raise(Kafka::DeliveryFailed) { producer.deliver_messages }

    assert_equal 2, producer.pending_message_count
  end

  class SlowProducer < DummyProducer
    attr_reader :max_concurrent_deliveries

    def initialize
      @mutex = Mutex.new
      @deliveries = 0
      @max_concurrent_deliveries = 0
    end

    def deliver_messages
      @mutex.synchronize {
        @deliveries += 1
        @max_concurrent_deliveries = [@max_concurrent_deliveries, @deliveries].max
      }
      sleep 0.1
      @mutex.synchronize { @deliveries -= 1 }
    end

    def shutdown
    end
  end

  def test_shared_producer_delivers_one_chunk_at_a_time
    d = create_driver(config + config_element('ROOT', '', {"share_producer" => true}))
    producer = SlowProducer.new
    stub(d.instance).create_producer { producer }
    d.instance_start

    2.times.map {
      Thread.new { d.instance.write(create_chunk([{"a" => "b"}])) }
    }.each(&:join)

    assert_equal 1, producer.max_concurrent_deliveries
  ensure
    d.instance_shutdown if d
  end

  data("crc32" => "crc32",
       "murmur2" => "murmur2")
  def test_partitioner_hash_function(data)
    hash_type = data
    d = create_driver(config + config_element('ROOT', '', {"partitioner_hash_function" => hash_type}))
    assert_nothing_raised do
      d.instance.refresh_client
    end
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?
  end

  def test_resolve_seed_brokers
    d = create_driver(config + config_element('ROOT', '', {"resolve_seed_brokers" => true}))
    assert_nothing_raised do
      d.instance.refresh_client
    end
  end

  class WriteTest < self
    TOPIC_NAME = "kafka-output-#{SecureRandom.uuid}"

    INPUT_CONFIG = %[
      @type kafka
      brokers localhost:9092
      format json
      @label @kafka
      topics #{TOPIC_NAME}
    ]

    def create_target_driver(conf = INPUT_CONFIG)
      Fluent::Test::Driver::Input.new(Fluent::KafkaInput).configure(conf)
    end

    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
    end

    def teardown
      @kafka.delete_topic(TOPIC_NAME)
      @kafka.close
    end

    def test_write
      target_driver = create_target_driver
      expected_message = {"a" => 2}
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(config(default_topic: TOPIC_NAME))
        d.run do
          d.feed("test", event_time, expected_message)
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([expected_message], actual_messages)
    end

    def test_write_with_integer_keys
      target_driver = create_target_driver(INPUT_CONFIG + "kafka_message_key kafka_key\n")
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(config(default_topic: TOPIC_NAME))
        d.run do
          d.feed("test", event_time, {"a" => 2, "message_key" => 123, "partition_key" => 456})
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([{"a" => 2, "message_key" => 123, "partition_key" => 456, "kafka_key" => "123"}], actual_messages)
    end

    def test_record_key
      conf = config(default_topic: TOPIC_NAME) +
        config_element('ROOT', '', {"record_key" => "$.data"}, [])
      target_driver = create_target_driver
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(conf)
        d.run do
          d.feed('test', event_time, {'data' => {'a' => 'b', 'foo' => 'bar', 'message' => 'test'}, 'message_key' => '123456'})
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([{'a' => 'b', 'foo' => 'bar', 'message' => 'test'}], actual_messages)
    end

    def test_exclude_fields
      conf = config(default_topic: TOPIC_NAME) +
             config_element('ROOT', '', {"exclude_fields" => "$.foo"}, [])
      target_driver = create_target_driver
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(conf)
        d.run do
          d.feed('test', event_time, {'a' => 'b', 'foo' => 'bar', 'message' => 'test'})
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([{'a' => 'b', 'message' => 'test'}], actual_messages)
    end
  end
end
