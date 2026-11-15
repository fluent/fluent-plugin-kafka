require 'helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'securerandom'

class KafkaGroupInputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

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

  def test_process_batch_parses_time_format_as_utc
    d = create_driver(%[
      brokers localhost:9092
      consumer_group fluentd
      format json
      topics #{TOPIC_NAME}
      time_source record
      time_format %Y-%m-%dT%H:%M:%S.%NZ
      utc true
    ])
    message = Struct.new(:value, :key, :offset, :create_time, :headers).new(
      {'time' => '2020-11-11T07:58:58.7291622Z'}.to_json, nil, 0, Time.now, {})
    batch = Struct.new(:topic, :partition, :messages).new(TOPIC_NAME, 0, [message])
    emitted = nil
    stub(d.instance).emit_events { |tag, es| emitted = es }

    with_timezone("JST-9") do
      d.instance.process_batch(batch)
    end

    time, _record = emitted.first
    assert_equal Fluent::EventTime.new(Time.utc(2020, 11, 11, 7, 58, 58).to_i, 729162200), time
  end

  class MessageEncodingTest < self
    FakeMessage = Struct.new(:value, :key, :offset, :create_time, :headers)
    FakeBatch = Struct.new(:topic, :partition, :messages)

    def process(conf, messages, with_record_tag: false)
      d = create_driver(%[
        brokers localhost:9092
        consumer_group fluentd
        topics #{TOPIC_NAME}
        kafka_message_key key
        add_headers true
        #{conf}
      ])
      emitted = []
      stub(d.instance).emit_events { |tag, es| es.each { |_time, record| emitted << [tag, record] } }
      batch = FakeBatch.new(TOPIC_NAME, 0, messages)
      if with_record_tag
        d.instance.process_batch_with_record_tag(batch)
      else
        d.instance.process_batch(batch)
      end
      emitted
    end

    data("text" => ["text", "あ", "\xFF"],
         "ltsv" => ["ltsv", "message:あ", "message:\xFF"],
         "custom parser" => ["none", "あ", "\xFF"])
    def test_marks_valid_utf8_as_utf8(data)
      format, valid_value, invalid_value = data
      emitted = process("format #{format}",
                        [FakeMessage.new(valid_value.b, 'い'.b, 0, Time.now, {'う'.b => 'え'.b}),
                         FakeMessage.new(invalid_value.b, "\xFF".b, 1, Time.now, {"\xFF".b => "\xFF".b})])

      assert_equal([[TOPIC_NAME, {'message' => 'あ', 'key' => 'い', 'う' => 'え'}],
                    [TOPIC_NAME, {'message' => "\xFF".b, 'key' => "\xFF".b, "\xFF".b => "\xFF".b}]],
                   emitted)
    end

    def test_marks_valid_utf8_key_and_headers_as_utf8_with_record_tag
      emitted = process("format json\ntag_source record",
                        [FakeMessage.new('{"tag":"app"}', 'い'.b, 0, Time.now, {'う'.b => 'え'.b}),
                         FakeMessage.new('{"tag":"app"}', "\xFF".b, 1, Time.now, {"\xFF".b => "\xFF".b})],
                        with_record_tag: true)

      assert_equal([['app', {'tag' => 'app', 'key' => 'い', 'う' => 'え'}],
                    ['app', {'tag' => 'app', 'key' => "\xFF".b, "\xFF".b => "\xFF".b}]],
                   emitted)
    end
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
      d = @driver = create_driver(conf)
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

    data("time key removed by the parser" => ["", false, false],
         "keep_time_key" => ["keep_time_key true", true, false],
         "time key removed by the parser, record tag" => ["", false, true],
         "keep_time_key, record tag" => ["keep_time_key true", true, true])
    def test_custom_parser_time(data)
      extra_conf, keeps_time_key, with_record_tag = data
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format regexp
        expression /^(?<time>\\S+ \\S+ \\S+) (?<tag>\\S+) (?<message>.*)$/
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        #{extra_conf}
        #{with_record_tag ? 'tag_source record' : ''}
        topics #{TOPIC_NAME}
      ]
      emitted = process(conf, ['2020-11-11 07:58:58 +0000 app record 1',
                               'not a time app record 2',
                               '2020-11-11 07:59:00 +0000 app record 3'],
                        with_record_tag: with_record_tag)

      assert_equal(['record 1', 'record 3'], emitted.map { |_, _, r| r['message'] })
      assert_equal([with_record_tag ? 'app' : TOPIC_NAME] * 2, emitted.map { |tag, _, _| tag })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58)),
                    Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 59, 0))],
                   emitted.map { |_, t, _| t })
      assert_equal([keeps_time_key, keeps_time_key], emitted.map { |_, _, r| r.has_key?('time') })
    end

    def test_apache2_parser_time
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format apache2
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        topics #{TOPIC_NAME}
      ]
      emitted = process(conf, ['192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"'])

      assert_equal(['/'], emitted.map { |_, _, r| r['path'] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2013, 2, 28, 3, 0, 0))], emitted.map { |_, t, _| t })
    end

    def test_custom_parser_time_key_other_than_record_time_key
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format regexp
        expression /^(?<ts>\\S+ \\S+ \\S+) (?<message>.*)$/
        time_key ts
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        topics #{TOPIC_NAME}
      ]
      emitted = process(conf, ['2020-11-11 07:58:58 +0000 record 1'])

      assert_equal([{'message' => 'record 1'}], emitted.map { |_, _, r| r })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))], emitted.map { |_, t, _| t })
    end

    def test_custom_parser_keeps_record_time_key
      conf = %[
        brokers localhost:9092
        consumer_group fluentd
        format none
        message_key time
        time_key time
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        topics #{TOPIC_NAME}
      ]
      emitted = process(conf, ['2020-11-11 07:58:58 +0000', 'bad'])

      assert_equal([{'time' => '2020-11-11 07:58:58 +0000'}], emitted.map { |_, _, r| r })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))], emitted.map { |_, t, _| t })
      assert_true @driver.logs.any? { |log| log.include?("parser error") && log.include?("bad") }
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

  class RecordTagTest < self
    FakeMessage = Struct.new(:value, :key, :offset, :create_time, :headers)
    FakeBatch = Struct.new(:topic, :partition, :messages)

    class FakeRouter
      attr_reader :emitted

      def initialize
        @emitted = []
      end

      def emit_stream(tag, es)
        records = []
        es.each { |time, record| records << record['message'] }
        @emitted << [tag, records]
      end
    end

    def tagged_message(tag, message, offset)
      FakeMessage.new({'tag' => tag, 'message' => message}.to_json, nil, offset, Time.now, {})
    end

    def process(messages, extra_conf = '')
      d = @driver = create_driver(%[
        brokers localhost:9092
        consumer_group fluentd
        format json
        tag_source record
        topics #{TOPIC_NAME}
        #{extra_conf}
      ])
      router = FakeRouter.new
      stub(d.instance).router { router }
      d.instance.process_batch_with_record_tag(FakeBatch.new(TOPIC_NAME, 0, messages))
      router.emitted
    end

    data("without prefix and suffix" => ['',
                                          [['app.a', ['record 1']],
                                           [nil, ['record 3']],
                                           ['', ['record 4']],
                                           ['app.b', ['record 7']]],
                                          []],
         "with prefix and suffix" => ["add_prefix prefix\nadd_suffix suffix",
                                      [['prefix.app.a.suffix', ['record 1']],
                                       ['prefix..suffix', ['record 4']],
                                       ['prefix.app.b.suffix', ['record 7']]],
                                      ['offset=2']])
    def test_skips_invalid_tag(data)
      extra_conf, expected_emitted, expected_parser_errors = data
      emitted = process([tagged_message('app.a', 'record 1', 0),
                         tagged_message(7, 'record 2', 1),
                         FakeMessage.new({'message' => 'record 3'}.to_json, nil, 2, Time.now, {}),
                         tagged_message('', 'record 4', 3),
                         tagged_message({'name' => 'app'}, 'record 5', 4),
                         FakeMessage.new("{\"tag\":\"app.\xFF\",\"message\":\"record 6\"}".b, nil, 5, Time.now, {}),
                         tagged_message('app.b', 'record 7', 6)],
                        extra_conf)

      assert_equal(expected_emitted, emitted)
      warnings = @driver.logs.select { |log| log.include?("skipped record with invalid tag in #{TOPIC_NAME}/0") }
      assert_equal(['offset=1', 'offset=4', 'offset=5'],
                   warnings.map { |log| log[/offset=\d+/] })
      parser_errors = @driver.logs.select { |log| log.include?("parser error in #{TOPIC_NAME}/0") }
      assert_equal(expected_parser_errors,
                   parser_errors.map { |log| log[/offset=\d+/] })
    end

    # An output may change the tag in place, as HandleTagNameMixin#filter_record does.
    def test_emits_tag_that_can_be_changed_in_place
      d = create_driver(%[
        brokers localhost:9092
        consumer_group fluentd
        format json
        tag_source record
        add_prefix kafka
        topics #{TOPIC_NAME}
      ])
      emitted = []
      router = Object.new
      router.define_singleton_method(:emit_stream) do |tag, _es|
        frozen = tag.frozen?
        tag.sub!(/\Akafka\./, '')
        emitted << [tag, frozen]
      end
      stub(d.instance).router { router }

      assert_nothing_raised {
        d.instance.process_batch_with_record_tag(FakeBatch.new(TOPIC_NAME, 0, [tagged_message('app.a', 'record 1', 0)]))
      }
      assert_equal([['app.a', false]], emitted)
    end

    def test_retry_emits_tag_from_before_the_change
      d = create_driver(%[
        brokers localhost:9092
        consumer_group fluentd
        format json
        tag_source record
        add_prefix kafka
        retry_emit_limit 1
        topics #{TOPIC_NAME}
      ])
      tags = []
      router = Object.new
      router.define_singleton_method(:emit_stream) do |tag, _es|
        tags << tag.dup
        tag = +tag
        tag.sub!(/\Akafka\./, '')
        raise Fluent::Plugin::Buffer::BufferOverflowError, "buffer space has too many data" if tags.size == 1
      end
      stub(d.instance).router { router }
      stub(d.instance).sleep
      d.instance.instance_variable_set(:@consumer, Object.new)

      d.instance.process_batch_with_record_tag(FakeBatch.new(TOPIC_NAME, 0, [tagged_message('app.a', 'record 1', 0)]))
      assert_equal(['kafka.app.a', 'kafka.app.a'], tags)
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
