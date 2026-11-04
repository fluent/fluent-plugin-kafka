require 'helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'securerandom'
require 'json'

class KafkaInputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

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

  data("localtime" => ["", Time.new(2020, 11, 11, 7, 58, 58, "+09:00")],
       "utc" => ["utc true", Time.utc(2020, 11, 11, 7, 58, 58)],
       "timezone" => ["timezone -05:00", Time.new(2020, 11, 11, 7, 58, 58, "-05:00")],
       "timezone takes precedence over utc" => ["utc true\ntimezone -05:00", Time.new(2020, 11, 11, 7, 58, 58, "-05:00")])
  def test_configure_time_zone_of_time_format(data)
    params, expected = data
    with_timezone("JST-9") do
      d = create_driver(CONFIG + %[
        time_source record
        time_format %Y-%m-%d %H:%M:%S
        #{params}
      ])
      time_parser = d.instance.instance_variable_get(:@time_parser)
      assert_equal Fluent::EventTime.from_time(expected), time_parser.parse('2020-11-11 07:58:58')
    end
  end

  def test_configure_invalid_timezone
    assert_raise(Fluent::ConfigError) {
      create_driver(CONFIG + %[
        time_source record
        time_format %Y-%m-%d %H:%M:%S
        timezone Invalid/Zone
      ])
    }
  end

  class TopicWatcherTest < self
    FakeMessage = Struct.new(:value, :key, :offset, :create_time)

    class FakeRouter
      attr_reader :emitted, :emitted_times

      def initialize
        @emitted = []
        @emitted_times = []
      end

      def emit_stream(tag, es)
        records = []
        es.each { |time, record|
          records << record
          @emitted_times << time
        }
        @emitted << [tag, records]
      end
    end

    class OverflowRouter < FakeRouter
      def initialize(overflow_tag)
        super()
        @overflow_tag = overflow_tag
      end

      def emit_stream(tag, es)
        raise Fluent::KafkaInput::BufferChunkOverflowError, "too large" if tag == @overflow_tag
        super
      end
    end

    def create_topic_watcher(messages, router, tag_source: :record, add_prefix: nil, add_suffix: nil, time_source: :now, time_parser: nil, parser: nil)
      kafka = Object.new
      kafka.define_singleton_method(:fetch_messages) { |**args| messages }
      parser ||= Proc.new { |msg, te| JSON.parse(msg.value) }

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
        time_source,
        'time',
        time_parser,
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
        tagged_message('', 'record 4', 3),
        tagged_message('attacker.controlled', 'record 5', 4),
      ]
      router = FakeRouter.new
      watcher = create_topic_watcher(messages, router)
      watcher.consume

      assert_equal([['app.trusted', ['record 1']],
                    ['attacker.controlled', ['record 5']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal(5, watcher.instance_variable_get(:@next_offset))
    end

    def test_consume_advances_offset_when_every_message_is_skipped
      messages = [
        FakeMessage.new('this is not json', nil, 0, Time.now),
        tagged_message(nil, 'record 1', 1),
      ]
      router = FakeRouter.new
      watcher = create_topic_watcher(messages, router)
      watcher.consume

      assert_equal([], router.emitted)
      assert_equal(2, watcher.instance_variable_get(:@next_offset))
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

    def test_consume_with_time_source_record_parses_time_format
      messages = [
        FakeMessage.new({'tag' => 'app', 'time' => '2020-11-11 07:58:58 +0000'}.to_json, nil, 0, Time.now),
      ]
      router = FakeRouter.new
      time_parser = Fluent::TimeParser.new('%Y-%m-%d %H:%M:%S %z')
      create_topic_watcher(messages, router, time_source: :record, time_parser: time_parser).consume

      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))], router.emitted_times)
    end

    def test_consume_with_time_source_record_parses_non_string_time
      messages = [
        FakeMessage.new({'tag' => 'app', 'time' => 1605081538}.to_json, nil, 0, Time.now),
      ]
      router = FakeRouter.new
      time_parser = Fluent::TimeParser.new('%s')
      create_topic_watcher(messages, router, time_source: :record, time_parser: time_parser).consume

      assert_equal([Fluent::EventTime.new(1605081538)], router.emitted_times)
    end

    def test_consume_with_time_source_record_uses_current_time_for_missing_time_key
      messages = [
        FakeMessage.new({'tag' => 'app', 'message' => 'record 1'}.to_json, nil, 0, Time.now),
      ]
      router = FakeRouter.new
      now = Fluent::EventTime.now
      stub(Fluent::Engine).now { now }
      time_parser = Fluent::TimeParser.new('%Y-%m-%d %H:%M:%S %z')
      create_topic_watcher(messages, router, time_source: :record, time_parser: time_parser).consume

      assert_equal([['app', ['record 1']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal([now], router.emitted_times)
    end

    def test_consume_with_time_source_record_skips_time_not_matching_time_format
      messages = [
        FakeMessage.new({'tag' => 'app', 'message' => 'record 1', 'time' => '2020-11-11 07:58:58 +0000'}.to_json, nil, 0, Time.now),
        FakeMessage.new({'tag' => 'app', 'message' => 'record 2', 'time' => 1605081538}.to_json, nil, 1, Time.now),
        FakeMessage.new({'tag' => 'app', 'message' => 'record 3', 'time' => '2020-11-11 07:59:00 +0000'}.to_json, nil, 2, Time.now),
      ]
      router = FakeRouter.new
      time_parser = Fluent::TimeParser.new('%Y-%m-%d %H:%M:%S %z')
      watcher = create_topic_watcher(messages, router, time_source: :record, time_parser: time_parser)
      watcher.consume

      assert_equal([['app', ['record 1', 'record 3']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58)),
                    Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 59, 0))],
                   router.emitted_times)
      assert_equal(3, watcher.instance_variable_get(:@next_offset))
    end

    data("time key removed by the parser" => ["", false],
         "keep_time_key" => ["keep_time_key true", true])
    def test_consume_with_time_source_record_and_custom_parser(data)
      extra_conf, keeps_time_key = data
      conf = %[
        @type kafka
        brokers localhost:9092
        format regexp
        expression /^(?<time>\\S+ \\S+ \\S+) (?<message>.*)$/
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        #{extra_conf}
        topics #{TOPIC_NAME}
      ]
      input = create_driver(conf).instance
      messages = [
        FakeMessage.new('2020-11-11 07:58:58 +0000 record 1', nil, 0, Time.now),
        FakeMessage.new('not a time record 2', nil, 1, Time.now),
        FakeMessage.new('2020-11-11 07:59:00 +0000 record 3', nil, 2, Time.now),
      ]
      router = FakeRouter.new
      watcher = create_topic_watcher(messages, router, tag_source: :topic, time_source: :record,
                                     time_parser: input.instance_variable_get(:@time_parser),
                                     parser: input.instance_variable_get(:@parser_proc))
      watcher.consume

      assert_equal([[TOPIC_NAME, ['record 1', 'record 3']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58)),
                    Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 59, 0))],
                   router.emitted_times)
      assert_equal([keeps_time_key, keeps_time_key],
                   router.emitted[0][1].map { |r| r.has_key?('time') })
      assert_equal(3, watcher.instance_variable_get(:@next_offset))
    end

    def test_consume_skips_buffer_chunk_overflow
      messages = [
        tagged_message('app.large', 'record 1', 0),
        tagged_message('app.small', 'record 2', 1),
      ]
      router = OverflowRouter.new('app.large')
      watcher = create_topic_watcher(messages, router)
      watcher.consume

      assert_equal([['app.small', ['record 2']]],
                   router.emitted.map { |tag, records| [tag, records.map { |r| r['message'] }] })
      assert_equal(2, watcher.instance_variable_get(:@next_offset))
    end
  end

  class ConsumeTest < self
    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @producer = @kafka.producer
      # Use a new topic for each test, so that a test can read it from offset 0
      # without seeing messages left over from another test.
      @topic = "kafka-input-#{SecureRandom.uuid}"
    end

    def teardown
      @kafka.delete_topic(@topic)
      @kafka.close
    end

    def test_consume
      conf = %[
        @type kafka
        brokers localhost:9092
        format text
        @label @kafka
        topics #{@topic}
      ]
      d = create_driver(conf)

      d.run(expect_records: 1, timeout: 10) do
        @producer.produce("Hello, fluent-plugin-kafka!", topic: @topic)
        @producer.deliver_messages
      end
      expected = {'message'  => 'Hello, fluent-plugin-kafka!'}
      assert_equal expected, d.events[0][2]
    end

    data("time zone in time_format" => ["%Y-%m-%d %H:%M:%S %z", "", "2020-11-11 07:58:58 +0000", Time.utc(2020, 11, 11, 7, 58, 58)],
         "utc" => ["%Y-%m-%d %H:%M:%S", "utc true", "2020-11-11 07:58:58", Time.utc(2020, 11, 11, 7, 58, 58)],
         "timezone" => ["%Y-%m-%d %H:%M:%S", "timezone -05:00", "2020-11-11 07:58:58", Time.new(2020, 11, 11, 7, 58, 58, "-05:00")])
    def test_consume_with_time_source_record_and_time_format(data)
      time_format, params, time, expected = data
      conf = %[
        @type kafka
        brokers localhost:9092
        format json
        time_source record
        time_format #{time_format}
        #{params}
        offset 0
        @label @kafka
        topics #{@topic}
      ]
      with_timezone("JST-9") do
        d = create_driver(conf)

        d.run(expect_records: 1, timeout: 10) do
          @producer.produce({'time' => time}.to_json, topic: @topic)
          @producer.deliver_messages
        end
        assert_equal Fluent::EventTime.from_time(expected), d.events[0][1]
      end
    end
  end
end
