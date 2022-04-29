require 'helper'
require 'fluent/test/helpers'
require 'fluent/test/driver/input'
require 'fluent/test/driver/output'
require 'securerandom'

class Rdkafka2OutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def have_rdkafka
    begin
      require 'fluent/plugin/out_rdkafka2'
      true
    rescue LoadError
      false
    end
  end

  def setup
    omit_unless(have_rdkafka, "rdkafka isn't installed")
    Fluent::Test.setup
  end

  def base_config
    config_element('ROOT', '', {"@type" => "rdkafka2"}, [
                     config_element('format', "", {"@type" => "json"})
                   ])
  end

  def config(default_topic: "kitagawakeiko")
    base_config + config_element('ROOT', '', {"default_topic" => default_topic,
                                              "brokers" => "localhost:9092"}, [
                                 ])
  end

  def create_driver(conf = config, tag='test')
    Fluent::Test::Driver::Output.new(Fluent::Rdkafka2Output).configure(conf)
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
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?
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
      @kafka = nil
      omit_unless(have_rdkafka, "rdkafka isn't installed")
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
    end

    def teardown
      if @kafka
        @kafka.delete_topic(TOPIC_NAME)
        @kafka.close
      end
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

    def test_write_with_use_event_time
      input_config = %[
        @type kafka
        brokers localhost:9092
        format json
        @label @kafka
        topics #{TOPIC_NAME}
        time_source kafka
      ]
      target_driver = create_target_driver(input_config)
      expected_message = {"a" => 2}
      now = event_time
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(config(default_topic: TOPIC_NAME) + config_element('ROOT', '', {"use_event_time" => true}))
        d.run do
          d.feed("test", now, expected_message)
        end
      end
      actual_time = target_driver.events.collect { |event| event[1] }.last
      assert_in_delta(actual_time, now, 0.001) # expects millseconds precision
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([expected_message], actual_messages)
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

    def test_max_enqueue_bytes_per_second
      conf = config(default_topic: TOPIC_NAME) +
             config_element('ROOT', '', {"max_enqueue_bytes_per_second" => 32 * 3}, [])
      target_driver = create_target_driver
      expected_messages = []
      target_driver.run(expect_records: 9, timeout: 10) do
        sleep 2
        d = create_driver(conf)
        start_time = Fluent::Clock.now
        d.run do
          9.times do |i|
            message = {"message" => "32bytes message: #{i}"}
            d.feed("test", event_time, message)
            expected_messages << message
          end
        end
        assert_in_delta(2.0, Fluent::Clock.now - start_time, 0.5)
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal(expected_messages, actual_messages)
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
  end
end
