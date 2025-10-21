require 'helper'
require 'fluent/test/driver/input'
require 'securerandom'

class RdkafkaGroupInputTest < Test::Unit::TestCase

  def have_rdkafka
    begin
      require 'fluent/plugin/in_rdkafka_group'
      true
    rescue LoadError
      false
    end
  end

  def setup
    omit_unless(have_rdkafka, "rdkafka isn't installed")
    Fluent::Test.setup
  end

  TOPIC_NAME = "kafka-input-#{SecureRandom.uuid}"

  CONFIG = %[
    topics #{TOPIC_NAME}
    kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
    <parse>
      @type none
    </parse>
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::RdKafkaGroupInput).configure(conf)
  end


  def test_configure
    d = create_driver
    assert_equal [TOPIC_NAME], d.instance.topics
    assert_equal 'localhost:9092', d.instance.kafka_configs['bootstrap.servers']
  end

  def test_multi_worker_support
    d = create_driver
    assert_true d.instance.multi_workers_ready?
  end

  class ConsumeTest < self
    TOPIC_NAME = "kafka-input-#{SecureRandom.uuid}"

    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @producer = @kafka.producer
      @kafka.create_topic(TOPIC_NAME)
    end

    def teardown
      @kafka.delete_topic(TOPIC_NAME)
      @kafka.close
    end

    def test_consume
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        <parse>
          @type none
        </parse>
      ]

      d = create_driver(conf)

      d.run(expect_records: 1, timeout: 10) do
        sleep 0.1
        @producer.produce("Hello, fluent-plugin-kafka!", topic: TOPIC_NAME)
        @producer.deliver_messages
      end

      expected = {'message'  => 'Hello, fluent-plugin-kafka!'}
      assert_equal expected, d.events[0][2]
    end
  end

  class ConsumeTopicWithRegexpTest < self
    TOPIC_NAME1       = "kafka-input-1-#{SecureRandom.uuid}"
    TOPIC_NAME2       = "kafka-input-2-#{SecureRandom.uuid}"
    TOPIC_NAME_REGEXP = "/kafka-input-(1|2)-.*/"

    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @producer = @kafka.producer
      @kafka.create_topic(TOPIC_NAME1)
      @kafka.create_topic(TOPIC_NAME2)
    end

    def teardown
      @kafka.delete_topic(TOPIC_NAME1)
      @kafka.delete_topic(TOPIC_NAME2)
      @kafka.close
    end

    def test_consume_with_regexp
      conf = %[
        topics #{TOPIC_NAME_REGEXP}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        <parse>
          @type none
        </parse>
      ]
      d = create_driver(conf)

      d.run(expect_records: 2, timeout: 10) do
        sleep 0.1
        @producer.produce("Hello, fluent-plugin-kafka! in topic 1", topic: TOPIC_NAME1)
        @producer.produce("Hello, fluent-plugin-kafka! in topic 2", topic: TOPIC_NAME2)
        @producer.deliver_messages
      end
      expected_message_pattern = /Hello, fluent-plugin-kafka! in topic [12]/
      assert_equal 2, d.events.size
      assert_match(expected_message_pattern, d.events[0][2]['message'])
      assert_match(expected_message_pattern, d.events[1][2]['message'])
    end
  end
end
