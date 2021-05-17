require 'helper'
require 'fluent/test/driver/input'
require 'securerandom'

class KafkaInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TOPIC_NAME = "kafka-input-#{SecureRandom.uuid}"

  CONFIG = %[
    @type kafka
    brokers localhost:9092
    format text
    @label @kafka
    topics #{TOPIC_NAME}
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::KafkaInput).configure(conf)
  end


  def test_configure
    d = create_driver
    assert_equal TOPIC_NAME, d.instance.topics
    assert_equal 'text', d.instance.format
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_multi_worker_support
    d = create_driver
    assert_false d.instance.multi_workers_ready?
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
      conf = %[
        @type kafka
        brokers localhost:9092
        format text
        @label @kafka
        topics #{TOPIC_NAME}
      ]
      d = create_driver

      d.run(expect_records: 1, timeout: 10) do
        @producer.produce("Hello, fluent-plugin-kafka!", topic: TOPIC_NAME)
        @producer.deliver_messages
      end
      expected = {'message'  => 'Hello, fluent-plugin-kafka!'}
      assert_equal expected, d.events[0][2]
    end
  end
end
