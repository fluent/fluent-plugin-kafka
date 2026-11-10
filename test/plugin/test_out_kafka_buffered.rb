require 'helper'
require 'fluent/output'
require 'fluent/test/helpers'

class KafkaBufferedOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  BASE_CONFIG = %[
    type kafka_buffered
  ]

  CONFIG = BASE_CONFIG + %[
    default_topic kitagawakeiko
    brokers localhost:9092
  ]

  def create_driver(conf = CONFIG, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::KafkaOutputBuffered, tag).configure(conf)
  end

  # ruby-kafka only rejects these values in deliver_messages, where the
  # partition is packed as int32 and the default partitioner runs
  # Zlib.crc32 on the keys
  class DummyProducer
    attr_reader :produced

    def produce_for_buffered(value, key:, topic:, partition:, partition_key:, create_time:)
      @produced ||= []
      @produced << {value: value, key: key, partition_key: partition_key, partition: partition}
    end

    def deliver_messages
      (@produced || []).each do |message|
        [message[:partition]].pack("l>") if message[:partition]
        [message[:key], message[:partition_key]].compact.each { |key| Zlib.crc32(key) }
      end
    end
  end

  def write_records(records)
    d = create_driver
    producer = DummyProducer.new
    stub(d.instance).get_producer { producer }
    metadata = Fluent::Plugin::Buffer::Metadata.new(nil, nil, {key: 'test'})
    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(metadata)
    chunk.append(records.map { |record| [event_time, record].to_msgpack })
    d.instance.write(chunk)
    producer.produced || []
  end

  data("non numeric partition" => "not-a-number",
       "hash partition" => {"x" => 1},
       "negative partition" => -2,
       "out of int32 range" => 2**31)
  def test_write_skips_event_with_invalid_partition(partition)
    produced = nil
    assert_nothing_raised {
      produced = write_records([{"a" => "b"}, {"a" => "c", "partition" => partition}, {"a" => "d"}])
    }

    assert_equal ['{"a":"b"}', '{"a":"d"}'], produced.collect { |message| message[:value] }
  end

  def test_write_keeps_event_with_zero_padded_partition
    produced = write_records([{"a" => "b", "partition" => "010"}])

    assert_equal [10], produced.collect { |message| message[:partition] }
  end

  data("integer" => [123, "123"],
       "float" => [1.5, "1.5"],
       "boolean" => [true, "true"],
       "string" => ["abc", "abc"])
  def test_write_converts_keys_to_string(data)
    value, expected = data
    produced = nil
    assert_nothing_raised {
      produced = write_records([
        {"message_key" => value, "partition_key" => value},
        {"message_key" => value},
        {"partition_key" => value},
      ])
    }

    assert_equal [[expected, expected], [expected, nil], [nil, expected]],
                 produced.collect { |message| [message[:key], message[:partition_key]] }
  end

  def test_configure
    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(BASE_CONFIG)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(CONFIG)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(CONFIG + %[
        buffer_type memory
      ])
    }

    d = create_driver
    assert_equal 'kitagawakeiko', d.instance.default_topic
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_format
    d = create_driver
  end

  data("crc32" => "crc32",
      "murmur2" => "murmur2")
  def test_partitioner_hash_function(data)
    hash_type = data
    d = create_driver(CONFIG + %[partitioner_hash_function #{hash_type}])
    assert_nothing_raised do
      d.instance.refresh_client
    end
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?

  end

  def test_write
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
  end
end
