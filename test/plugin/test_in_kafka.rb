require 'helper'
require 'fluent/test/driver/input'
require 'securerandom'
require 'json'

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

  class TopicWatcherTest < self
    FakeMessage = Struct.new(:value, :key, :offset, :create_time)

    class FakeRouter
      attr_reader :emitted

      def initialize
        @emitted = []
      end

      def emit_stream(tag, es)
        records = []
        es.each { |time, record| records << record }
        @emitted << [tag, records]
      end
    end

    def create_topic_watcher(messages, router, tag_source: :record, add_prefix: nil, add_suffix: nil)
      kafka = Object.new
      kafka.define_singleton_method(:fetch_messages) { |**args| messages }
      parser = Proc.new { |msg, te| JSON.parse(msg.value) }

      Fluent::KafkaInput::TopicWatcher.new(
        Fluent::KafkaInput::TopicEntry.new(TOPIC_NAME, 0, 0),
        kafka,
        1,
        parser,
        add_prefix,
        add_suffix,
        nil,
        router,
        nil,
        :now,
        'time',
        tag_source,
        'tag')
    end

    def tagged_message(tag, message, offset)
      FakeMessage.new({'tag' => tag, 'message' => message}.to_json, nil, offset, Time.now)
    end

    def test_consume_with_tag_source_record_emits_stream_per_tag
      messages = [
        tagged_message('app.trusted', 'record 1', 0),
        tagged_message('app.trusted', 'record 2', 1),
        tagged_message('attacker.controlled', 'record 3', 2),
      ]
      router = FakeRouter.new
      create_topic_watcher(messages, router).consume

      assert_equal([['app.trusted', ['record 1', 'record 2']],
                    ['attacker.controlled', ['record 3']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
    end

    def test_consume_with_tag_source_record_applies_prefix_and_suffix_per_tag
      messages = [
        tagged_message('app.trusted', 'record 1', 0),
        tagged_message('attacker.controlled', 'record 2', 1),
      ]
      router = FakeRouter.new
      create_topic_watcher(messages, router, add_prefix: 'prefix', add_suffix: 'suffix').consume

      assert_equal(['prefix.app.trusted.suffix', 'prefix.attacker.controlled.suffix'],
                   router.emitted.map(&:first))
    end

    def test_consume_with_tag_source_record_skips_unparsable_message
      messages = [
        tagged_message('app.trusted', 'record 1', 0),
        FakeMessage.new('this is not json', nil, 1, Time.now),
        tagged_message('attacker.controlled', 'record 2', 2),
      ]
      router = FakeRouter.new
      create_topic_watcher(messages, router).consume

      assert_equal([['app.trusted', ['record 1']],
                    ['attacker.controlled', ['record 2']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
    end

    def test_consume_with_tag_source_record_skips_invalid_tag
      messages = [
        tagged_message('app.trusted', 'record 1', 0),
        tagged_message(7, 'record 2', 1),
        FakeMessage.new({'message' => 'record 3'}.to_json, nil, 2, Time.now),
        tagged_message('attacker.controlled', 'record 4', 3),
      ]
      router = FakeRouter.new
      watcher = create_topic_watcher(messages, router)
      watcher.consume

      assert_equal([['app.trusted', ['record 1']],
                    ['attacker.controlled', ['record 4']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal(4, watcher.instance_variable_get(:@next_offset))
    end

    def test_consume_with_tag_source_topic_emits_single_stream
      messages = [
        tagged_message('app.trusted', 'record 1', 0),
        tagged_message('attacker.controlled', 'record 2', 1),
      ]
      router = FakeRouter.new
      create_topic_watcher(messages, router, tag_source: :topic).consume

      assert_equal([[TOPIC_NAME, ['record 1', 'record 2']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
    end
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
