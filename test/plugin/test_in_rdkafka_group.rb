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

  data("format text" => ["format text", Fluent::NotFoundPluginError],
       "<parse> with @type text" => ["<parse>\n @type text\n</parse>", Fluent::NotFoundPluginError],
       "no format and no <parse>" => ["", Fluent::ConfigError])
  def test_configure_without_usable_format(data)
    parser_conf, error = data
    assert_raise(error) do
      create_driver(%[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        #{parser_conf}
      ])
    end
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

  class MessageEncodingTest < self
    data("ltsv" => ["@type ltsv", "message:あ", "message:\xFF"],
         "custom parser" => ["@type none", "あ", "\xFF"],
         "text with the deprecated type parameter" => ["type text", "あ", "\xFF"])
    def test_marks_valid_utf8_as_utf8(data)
      parse_type, valid_value, invalid_value = data
      d = create_driver(%[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        kafka_message_key key
        add_headers true
        <parse>
          #{parse_type}
        </parse>
      ])
      router = RecordTimeTest::FakeRouter.new
      stub(d.instance).router { router }
      messages = [
        RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, valid_value.b, 'い'.b, 0, Time.now,
                                        {'う'.b => 'え'.b, 'か'.b => ['き'.b, 'く'.b]}),
        RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, invalid_value.b, "\xFF".b, 1, Time.now,
                                        {"\xFF".b => "\xFF".b}),
      ]
      d.instance.instance_variable_set(:@consumer, RecordTimeTest::FakeConsumer.new(d.instance, messages))
      d.instance.run

      assert_equal([{'message' => 'あ', 'key' => 'い', 'う' => 'え', 'か' => ['き', 'く']},
                    {'message' => "\xFF".b, 'key' => "\xFF".b, "\xFF".b => "\xFF".b}],
                   router.emitted.map { |_tag, _time, record| record })
    end
  end

  class MessageKeyTest < self
    data("format none" => ["format none", 'msg'],
         "the deprecated type text in <parse>" => ["<parse>\n type text\n</parse>", 'msg'],
         "<parse> with @type none" => ["<parse>\n @type none\n</parse>", 'message'])
    def test_top_level_message_key(data)
      parser_conf, expected_key = data
      d = create_driver(%[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        message_key msg
        #{parser_conf}
      ])
      router = RecordTimeTest::FakeRouter.new
      stub(d.instance).router { router }
      messages = [RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, 'hello', nil, 0, Time.now, {})]
      d.instance.instance_variable_set(:@consumer, RecordTimeTest::FakeConsumer.new(d.instance, messages))
      d.instance.run

      assert_equal([{expected_key => 'hello'}], router.emitted.map { |_tag, _time, record| record })
    end
  end

  class RecordTagTest < self
    class FakeRouter
      attr_reader :emitted, :frozen_tags

      def initialize
        @emitted = []
        @frozen_tags = []
      end

      def emit_stream(tag, es)
        records = []
        es.each { |time, record| records << record['message'] }
        @emitted << [tag, records]
        @frozen_tags << tag.frozen?
      end
    end

    def tagged_message(tag, message, offset)
      payload = {'tag' => tag, 'message' => message}.to_json
      RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, payload, nil, offset, Time.now, {})
    end

    def consume(messages, extra_conf = '', tag_source: 'record', parser: "@type json")
      d = @driver = create_driver(%[
        topics #{TOPIC_NAME}
        kafka_configs {"bootstrap.servers": "localhost:9092", "group.id": "test_group"}
        tag_source #{tag_source}
        <parse>
          #{parser}
        </parse>
        #{extra_conf}
      ])
      router = @router = FakeRouter.new
      stub(d.instance).router { router }
      d.instance.instance_variable_set(:@consumer, RecordTimeTest::FakeConsumer.new(d.instance, messages))
      d.instance.run
      router.emitted
    end

    def test_configure_defaults
      d = create_driver
      assert_equal :topic, d.instance.tag_source
      assert_equal 'tag', d.instance.record_tag_key
    end

    data("without prefix and suffix" => ['', ['app.a', 'app.b']],
         "with prefix and suffix" => ["add_prefix prefix\nadd_suffix suffix",
                                      ['prefix.app.a.suffix', 'prefix.app.b.suffix']])
    def test_emits_stream_per_tag(data)
      extra_conf, (tag_a, tag_b) = data
      emitted = consume([tagged_message('app.a', 'record 1', 0),
                         tagged_message('app.b', 'record 2', 1),
                         tagged_message('app.a', 'record 3', 2)],
                        extra_conf)

      assert_equal([[tag_a, ['record 1', 'record 3']],
                    [tag_b, ['record 2']]],
                   emitted)
    end

    def test_record_tag_key
      payload = {'tag' => 'ignored', 'route' => 'app.a', 'message' => 'record 1'}.to_json
      emitted = consume([RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, payload, nil, 0, Time.now, {})],
                        'record_tag_key route')

      assert_equal([['app.a', ['record 1']]], emitted)
    end

    def test_skips_invalid_tag
      emitted = consume([tagged_message('app.a', 'record 1', 0),
                         tagged_message(7, 'record 2', 1),
                         RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, {'message' => 'record 3'}.to_json, nil, 2, Time.now, {}),
                         tagged_message('', 'record 4', 3),
                         tagged_message({'name' => 'app'}, 'record 5', 4),
                         RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, "{\"tag\":\"app.\xFF\",\"message\":\"record 6\"}".b, nil, 5, Time.now, {}),
                         tagged_message('app.b', 'record 7', 6)])

      assert_equal([['app.a', ['record 1']], ['app.b', ['record 7']]], emitted)
      warnings = @driver.logs.select { |log| log.include?("skipped record with invalid tag in #{TOPIC_NAME}/0") }
      assert_equal(['offset=1', 'offset=2', 'offset=3', 'offset=4', 'offset=5'],
                   warnings.map { |log| log[/offset=\d+/] })
    end

    data("dot" => '.',
         "dot dot" => '..',
         "path traversal" => '../x',
         "slash" => 'a/b',
         "space" => 'a b',
         "250 characters" => 'a' * 250)
    def test_skips_tag_that_is_not_a_topic_name(tag)
      emitted = consume([tagged_message('app.a', 'record 1', 0),
                         tagged_message(tag, 'record 2', 1),
                         tagged_message('app.b', 'record 3', 2)])

      assert_equal([['app.a', ['record 1']], ['app.b', ['record 3']]], emitted)
      warnings = @driver.logs.select { |log| log.include?("skipped record with invalid tag in #{TOPIC_NAME}/0") }
      assert_equal(['offset=1'], warnings.map { |log| log[/offset=\d+/] })
    end

    def test_emits_tag_of_249_characters
      tag = 'a' * 249
      emitted = consume([tagged_message(tag, 'record 1', 0)])

      assert_equal([[tag, ['record 1']]], emitted)
    end

    def test_skips_record_that_is_not_a_map
      emitted = consume([tagged_message('app.a', 'record 1', 0),
                         RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, '"a tag string"', nil, 1, Time.now, {}),
                         tagged_message('app.b', 'record 3', 2)])

      assert_equal([['app.a', ['record 1']], ['app.b', ['record 3']]], emitted)
      warnings = @driver.logs.select { |log| log.include?("skipped record with invalid tag in #{TOPIC_NAME}/0") }
      assert_equal(['offset=1'], warnings.map { |log| log[/offset=\d+/] })
    end

    def test_skips_binary_tag_with_regexp_parser
      payloads = ["app.a record 1", "app.\xFF record 2".b, "app.b record 3"]
      messages = payloads.each_with_index.map { |payload, i|
        RecordTimeTest::FakeMessage.new(TOPIC_NAME, 0, payload.b, nil, i, Time.now, {})
      }
      emitted = consume(messages,
                        parser: "@type regexp\n expression /^(?<tag>[^ ]*) (?<message>.*)$/")

      assert_equal([['app.a', ['record 1']], ['app.b', ['record 3']]], emitted)
      warnings = @driver.logs.select { |log| log.include?("skipped record with invalid tag in #{TOPIC_NAME}/0") }
      assert_equal(['offset=1'], warnings.map { |log| log[/offset=\d+/] })
    end

    def test_tag_source_topic_emits_mutable_tag
      consume([tagged_message('app.a', 'record 1', 0)], 'add_prefix prefix', tag_source: 'topic')

      assert_equal([false], @router.frozen_tags)
    end

    def test_tag_source_record_emits_mutable_tag
      consume([tagged_message('app.a', 'record 1', 0)])

      assert_equal([false], @router.frozen_tags)
    end

    def test_tag_source_topic_ignores_tag_field
      emitted = consume([tagged_message('app.a', 'record 1', 0),
                         tagged_message('app.b', 'record 2', 1)],
                        tag_source: 'topic')

      assert_equal([[TOPIC_NAME, ['record 1', 'record 2']]], emitted)
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
