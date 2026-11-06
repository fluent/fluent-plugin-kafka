require 'helper'
require 'fluent/test/driver/input'
require 'fluent/test/helpers'
require 'securerandom'

class RdkafkaGroupInputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

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

  def test_build_config_without_security_parameters
    d = create_driver
    config = d.instance.build_config

    assert_equal 'localhost:9092', config[:"bootstrap.servers"]
    assert_equal 'test_group', config[:"group.id"]
    assert_equal 'PLAINTEXT', config[:"security.protocol"]
    assert_nil config[:"ssl.endpoint.identification.algorithm"]
    assert_nil config[:"sasl.mechanisms"]
  end

  def test_build_config_ssl_ca_certs_from_system
    d = create_driver(CONFIG + %[
      ssl_ca_certs_from_system true
    ])
    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal 'https', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
    assert_nil config[:"ssl.ca.location"]
  end

  def test_build_config_ssl_client_cert
    d = create_driver(CONFIG + %[
      ssl_ca_cert /path/to/ca_cert.pem
      ssl_client_cert /path/to/cert.pem
      ssl_client_cert_key /path/to/key.pem
      ssl_client_cert_key_password secret
    ])
    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal '/path/to/ca_cert.pem', config[:"ssl.ca.location"]
    assert_equal '/path/to/cert.pem', config[:"ssl.certificate.location"]
    assert_equal '/path/to/key.pem', config[:"ssl.key.location"]
    assert_equal 'secret', config[:"ssl.key.password"]
  end

  def test_build_config_ssl_verify_hostname_false
    d = create_driver(CONFIG + %[
      ssl_ca_certs_from_system true
      ssl_verify_hostname false
    ])
    config = d.instance.build_config

    assert_equal 'none', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
  end

  def test_build_config_sasl_plain_over_ssl
    d = create_driver(CONFIG + %[
      username testuser
      password testpass
      ssl_ca_certs_from_system true
    ])
    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'PLAIN', config[:"sasl.mechanisms"]
    assert_equal 'testuser', config[:"sasl.username"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  def test_configure_sasl_plain_without_ssl_raises
    assert_raise(Fluent::ConfigError) {
      create_driver(CONFIG + %[
        username testuser
        password testpass
      ])
    }
  end

  def test_build_config_sasl_plain_without_ssl_allowed_by_sasl_over_ssl
    d = create_driver(CONFIG + %[
      username testuser
      password testpass
      sasl_over_ssl false
    ])
    config = d.instance.build_config

    assert_equal 'SASL_PLAINTEXT', config[:"security.protocol"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  data("sha256" => ["sha256", "SCRAM-SHA-256"],
       "sha512" => ["sha512", "SCRAM-SHA-512"])
  def test_build_config_sasl_scram(data)
    mechanism, expected = data
    d = create_driver(CONFIG + %[
      username testuser
      password testpass
      scram_mechanism #{mechanism}
      ssl_ca_certs_from_system true
    ])
    config = d.instance.build_config

    assert_equal expected, config[:"sasl.mechanisms"]
    assert_equal 'SASL_SSL', config[:"security.protocol"]
  end

  def test_build_config_sasl_scram_without_credentials_warns
    d = create_driver(CONFIG + %[
      scram_mechanism sha256
      ssl_ca_certs_from_system true
    ])
    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_nil config[:"sasl.mechanisms"]
    assert_true d.logs.any? { |log| log.include?("scram_mechanism is ignored") }
  end

  def test_build_config_sasl_gssapi
    d = create_driver(CONFIG + %[
      principal kafka/host@REALM
      keytab /path/to/kafka.keytab
      service_name kafka
      ssl_ca_certs_from_system true
    ])
    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'GSSAPI', config[:"sasl.mechanisms"]
    assert_equal 'kafka/host@REALM', config[:"sasl.kerberos.principal"]
    assert_equal '/path/to/kafka.keytab', config[:"sasl.kerberos.keytab"]
    assert_equal 'kafka', config[:"sasl.kerberos.service.name"]
  end

  def test_build_config_kafka_configs_take_precedence
    conf = %[
      topics #{TOPIC_NAME}
      ssl_ca_certs_from_system true
      kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group", "security.protocol": "SASL_SSL", "ssl.endpoint.identification.algorithm": "none"}
      <parse>
        @type none
      </parse>
    ]
    d = create_driver(conf)
    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'none', config[:"ssl.endpoint.identification.algorithm"]
    assert_nil config["security.protocol"]
  end

  def test_configure_sasl_plaintext_in_kafka_configs_raises
    conf = %[
      topics #{TOPIC_NAME}
      kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group", "security.protocol": "SASL_PLAINTEXT", "sasl.mechanisms": "PLAIN", "sasl.username": "testuser", "sasl.password": "testpass"}
      <parse>
        @type none
      </parse>
    ]

    assert_raise(Fluent::ConfigError) {
      create_driver(conf)
    }
    assert_nothing_raised {
      create_driver(conf + "sasl_over_ssl false\n")
    }
  end

  data("uppercase" => "SASL_PLAINTEXT",
       "lowercase" => "sasl_plaintext")
  def test_configure_sasl_plaintext_in_kafka_configs_raises_regardless_of_case(protocol)
    conf = %[
      topics #{TOPIC_NAME}
      username testuser
      password testpass
      kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group", "security.protocol": "#{protocol}"}
      <parse>
        @type none
      </parse>
    ]

    assert_raise(Fluent::ConfigError) {
      create_driver(conf)
    }
  end

  def test_setup_consumer_uses_build_config
    d = create_driver(CONFIG + %[
      ssl_ca_certs_from_system true
    ])
    consumer = Object.new
    stub(consumer).subscribe
    rdkafka_config = Object.new
    stub(rdkafka_config).consumer { consumer }
    passed = nil
    stub(Rdkafka::Config).new { |config| passed = config; rdkafka_config }

    d.instance.setup_consumer

    assert_equal 'localhost:9092', passed[:"bootstrap.servers"]
    assert_equal 'SSL', passed[:"security.protocol"]
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
    FakeMessage = Struct.new(:topic, :partition, :payload, :key, :offset, :timestamp, :headers)

    class FakeRouter
      attr_reader :emitted

      def initialize
        @emitted = []
      end

      def emit_stream(tag, es)
        es.each { |time, record| @emitted << [tag, time, record] }
      end
    end

    # Feeds the messages to run, then stops it the way shutdown does.
    class FakeConsumer
      def initialize(plugin, messages)
        @plugin = plugin
        @messages = messages
      end

      def poll(_timeout)
        message = @messages.shift
        @plugin.instance_variable_set(:@consumer, nil) unless message
        message
      end
    end

    def consume(conf, payloads)
      d = @driver = create_driver(conf)
      router = FakeRouter.new
      stub(d.instance).router { router }
      messages = payloads.each_with_index.map { |p, i| FakeMessage.new(TOPIC_NAME, 0, p, nil, i, Time.now, {}) }
      d.instance.instance_variable_set(:@consumer, FakeConsumer.new(d.instance, messages))
      d.instance.run
      router.emitted
    end

    data("time key removed by the parser" => ["", false],
         "keep_time_key" => ["keep_time_key true", true])
    def test_custom_parser_time(data)
      extra_conf, keeps_time_key = data
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        <parse>
          @type regexp
          expression /^(?<time>\\S+ \\S+ \\S+) (?<message>.*)$/
          time_format %Y-%m-%d %H:%M:%S %z
          #{extra_conf}
        </parse>
      ]
      emitted = consume(conf, ['2020-11-11 07:58:58 +0000 record 1',
                               'not a time record 2',
                               '2020-11-11 07:59:00 +0000 record 3'])

      assert_equal(['record 1', 'record 3'], emitted.map { |_, _, r| r['message'] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58)),
                    Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 59, 0))],
                   emitted.map { |_, t, _| t })
      assert_equal([keeps_time_key, keeps_time_key], emitted.map { |_, _, r| r.has_key?('time') })
    end

    def test_apache2_parser_time
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        <parse>
          @type apache2
        </parse>
      ]
      emitted = consume(conf, ['192.168.0.1 - - [28/Feb/2013:12:00:00 +0900] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"'])

      assert_equal(['/'], emitted.map { |_, _, r| r['path'] })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2013, 2, 28, 3, 0, 0))], emitted.map { |_, t, _| t })
    end

    def test_custom_parser_time_key_other_than_record_time_key
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        <parse>
          @type regexp
          expression /^(?<ts>\\S+ \\S+ \\S+) (?<message>.*)$/
          time_key ts
          time_format %Y-%m-%d %H:%M:%S %z
        </parse>
      ]
      emitted = consume(conf, ['2020-11-11 07:58:58 +0000 record 1'])

      assert_equal([{'message' => 'record 1'}], emitted.map { |_, _, r| r })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))], emitted.map { |_, t, _| t })
    end

    def test_custom_parser_keeps_record_time_key
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        <parse>
          @type none
          message_key time
          time_key time
        </parse>
      ]
      emitted = consume(conf, ['2020-11-11 07:58:58 +0000', 'bad'])

      assert_equal([{'time' => '2020-11-11 07:58:58 +0000'}], emitted.map { |_, _, r| r })
      assert_equal([Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))], emitted.map { |_, t, _| t })
      assert_true @driver.logs.any? { |log| log.include?("parser error") && log.include?("bad") }
    end

    def test_missing_time_key_uses_current_time
      conf = %[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        time_source record
        time_format %Y-%m-%d %H:%M:%S %z
        <parse>
          @type json
        </parse>
      ]
      now = Fluent::EventTime.now
      stub(Fluent::Engine).now { now }
      emitted = consume(conf, [{'message' => 'no time'}.to_json,
                               {'message' => 'with time', 'time' => '2020-11-11 07:58:58 +0000'}.to_json,
                               {'message' => 'bad time', 'time' => 1605081538}.to_json])

      assert_equal(['no time', 'with time'], emitted.map { |_, _, r| r['message'] })
      assert_equal([now, Fluent::EventTime.from_time(Time.utc(2020, 11, 11, 7, 58, 58))],
                   emitted.map { |_, t, _| t })
    end
  end

  class BufferChunkOverflowTest < self
    # Like Fluentd, keeps the records that fit, and raises at the end of the first batch
    class OverflowingRouter < RecordTimeTest::FakeRouter
      def emit_stream(tag, es)
        super
        raise Fluent::Plugin::Buffer::BufferChunkOverflowError, "too large" if @emitted.size == 2
      end
    end

    def test_oversized_record_keeps_consumer_and_following_records
      d = create_driver(CONFIG + "max_batch_size 2\n")
      router = OverflowingRouter.new
      stub(d.instance).router { router }
      reconnected = false
      stub(d.instance).reconnect_consumer {
        reconnected = true
        d.instance.instance_variable_set(:@consumer, nil)
      }
      messages = %w[a b c].each_with_index.map { |payload, i|
        RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, payload, nil, i, Time.now, {})
      }
      d.instance.instance_variable_set(:@consumer, RecordTimeTest::FakeConsumer.new(d.instance, messages))

      d.instance.run

      assert_false reconnected
      assert_equal(%w[a b c], router.emitted.map { |_, _, record| record['message'] })
    end
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
    TOPIC_NAME2       = "kafka-input-22-#{SecureRandom.uuid}"
    UNMATCHED_TOPIC   = "kafka-input-333-#{SecureRandom.uuid}"

    TOPIC_NAME_REGEXP = "/kafka-input-[0-9]{1,2}-.*/"

    def setup
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
      @producer = @kafka.producer
      @kafka.create_topic(TOPIC_NAME1)
      @kafka.create_topic(TOPIC_NAME2)
      @kafka.create_topic(UNMATCHED_TOPIC)
    end

    def teardown
      @kafka.delete_topic(TOPIC_NAME1)
      @kafka.delete_topic(TOPIC_NAME2)
      @kafka.delete_topic(UNMATCHED_TOPIC)
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
        @producer.produce("Should be ignored", topic: UNMATCHED_TOPIC)
        @producer.deliver_messages
      end
      expected_message_pattern = /Hello, fluent-plugin-kafka! in topic [12]/
      assert_equal 2, d.events.size
      assert_match(expected_message_pattern, d.events[0][2]['message'])
      assert_match(expected_message_pattern, d.events[1][2]['message'])
    end
  end
end
