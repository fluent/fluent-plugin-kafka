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
