require 'helper'
require 'fluent/test/driver/input'
require 'securerandom'

class KafkaGroupInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TOPIC_NAME = "kafka-input-#{SecureRandom.uuid}"

  CONFIG = %[
    @type kafka
    brokers localhost:9092
    consumer_group fluentd
    format text
    refresh_topic_interval 0
    @label @kafka
    topics #{TOPIC_NAME}
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::KafkaGroupInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal [TOPIC_NAME], d.instance.topics
    assert_equal 'text', d.instance.format
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_multi_worker_support
    d = create_driver
    assert_true d.instance.multi_workers_ready?
  end

  class ConsumeTest < self
    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @producer = @kafka.producer
    end

    def teardown
      @kafka.delete_topic(TOPIC_NAME)
      @kafka.close
    end

    def test_consume
      d = create_driver

      d.run(expect_records: 1, timeout: 10) do
        @producer.produce("Hello, fluent-plugin-kafka!", topic: TOPIC_NAME)
        @producer.deliver_messages
      end
      expected = {'message'  => 'Hello, fluent-plugin-kafka!'}
      assert_equal expected, d.events[0][2]
    end
  end

  class ConsumeWithHeadersTest < self
    CONFIG_TEMPLATE = %(
    @type kafka
    brokers localhost:9092
    consumer_group fluentd
    format text
    refresh_topic_interval 0
    @label @kafka
    topics %<topic>s
    %<conf_adds>s
  ).freeze

    def topic_random
      "kafka-input-#{SecureRandom.uuid}"
    end

    def kafka_test_context(conf_adds: '', topic: topic_random, conf_template: CONFIG_TEMPLATE)
      kafka = Kafka.new(['localhost:9092'], client_id: 'kafka')
      producer = kafka.producer(required_acks: 1)

      config = format(conf_template, topic: topic, conf_adds: conf_adds)
      driver = create_driver(config)

      yield topic, producer, driver
    ensure
      kafka.delete_topic(topic)
      kafka.close
    end

    def test_with_headers_content_merged_into_record
      conf_adds = 'add_headers true'
      kafka_test_context(conf_adds: conf_adds) do |topic, producer, driver|
        driver.run(expect_records: 1, timeout: 5) do
          producer.produce('Hello, fluent-plugin-kafka!', topic: topic, headers: { header1: 'content1' })
          producer.deliver_messages
        end

        expected = { 'message' => 'Hello, fluent-plugin-kafka!',
                     'header1' => 'content1' }
        assert_equal expected, driver.events[0][2]
      end
    end

    def test_with_headers_content_merged_under_dedicated_key
      conf_adds = %(
        add_headers true
        headers_key kafka_headers
      )
      kafka_test_context(conf_adds: conf_adds) do |topic, producer, driver|
        driver.run(expect_records: 1, timeout: 5) do
          producer.produce('Hello, fluent-plugin-kafka!', topic: topic, headers: { header1: 'content1' })
          producer.deliver_messages
        end

        expected = { 'message' => 'Hello, fluent-plugin-kafka!',
                     'kafka_headers' => { 'header1' => 'content1' } }
        assert_equal expected, driver.events[0][2]
      end
    end
  end
end
