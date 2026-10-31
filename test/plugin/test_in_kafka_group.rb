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
