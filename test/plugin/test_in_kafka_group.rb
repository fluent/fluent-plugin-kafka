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

  def test_emit_events_with_buffer_chunk_overflow
    d = create_driver
    es = Fluent::MultiEventStream.new
    router = Object.new
    mock(router).emit_stream("test", es).once { raise Fluent::Plugin::Buffer::BufferChunkOverflowError, "too large" }
    stub(d.instance).router { router }

    assert_nothing_raised do
      d.instance.emit_events("test", es)
    end
    assert_true d.logs.any? { |log| log.include?("Skipped records larger than the buffer chunk limit size") && log.include?("too large") }
  end

  class RecordTimeTest < self
    FakeMessage = Struct.new(:value, :key, :offset, :create_time, :headers)
    FakeBatch = Struct.new(:topic, :partition, :messages)

    class FakeRouter
      attr_reader :emitted

      def initialize
        @emitted = []
      end

      def emit_stream(tag, es)
        es.each { |time, record| @emitted << [tag, time, record] }
      end
    end

    def message(value, offset)
      FakeMessage.new(value, nil, offset, Time.now, {})
    end

    def process(conf, values, with_record_tag: false)
      d = create_driver(conf)
      router = FakeRouter.new
      stub(d.instance).router { router }
      batch = FakeBatch.new(TOPIC_NAME, 0, values.each_with_index.map { |v, i| message(v, i) })
      if with_record_tag
        d.instance.process_batch_with_record_tag(batch)
      else
        d.instance.process_batch(batch)
      end
      router.emitted
    end

    data("time key removed by the parser" => ["", false],
         "keep_time_key" => ["keep_time_key true", true])
    def test_custom_parser_time(data)
      extra_conf, keeps_time_key = data
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format regexp
        expression /^(?<time>\\S+ \\S+ \\S+) (?<message>.*)$/
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        #{extra_conf}
        topics #{TOPIC_NAME}
      ]
      emitted = process(conf, ['2020-11-11 07:58:58 +0000 record 1',
                               'not a time record 2',
                               '2020-11-11 07:59:00 +0000 record 3'])

      assert_equal(['record 1', 'record 3'], emitted.map { |_, _, r| r['message'] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58)),
                    Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 59, 0))],
                   emitted.map { |_, t, _| t })
      assert_equal([keeps_time_key, keeps_time_key], emitted.map { |_, _, r| r.has_key?('time') })
    end

    data("topic tag" => false, "record tag" => true)
    def test_missing_time_key_uses_current_time(with_record_tag)
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format json
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        #{with_record_tag ? 'tag_source record' : ''}
        topics #{TOPIC_NAME}
      ]
      now = Fluent::EventTime.now
      stub(Fluent::Engine).now { now }
      emitted = process(conf, [{'tag' => 'app', 'message' => 'no time'}.to_json,
                               {'tag' => 'app', 'message' => 'with time', 'time' => '2020-11-11 07:58:58 +0000'}.to_json,
                               {'tag' => 'app', 'message' => 'bad time', 'time' => 1605081538}.to_json],
                        with_record_tag: with_record_tag)

      assert_equal(['no time', 'with time'], emitted.map { |_, _, r| r['message'] })
      assert_equal([now, Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))],
                   emitted.map { |_, t, _| t })
    end
  end

  class ConsumeTest < self
    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @kafka.create_topic(TOPIC_NAME)
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
        refresh_topic_interval 0
        topics #{TOPIC_NAME}
      ]
      d = create_driver

      d.run(expect_records: 1, timeout: 10) do
        sleep 0.1
        @producer.produce("Hello, fluent-plugin-kafka!", topic: TOPIC_NAME)
        @producer.deliver_messages
      end
      expected = {'message'  => 'Hello, fluent-plugin-kafka!'}
      assert_equal expected, d.events[0][2]
    end
  end
end
