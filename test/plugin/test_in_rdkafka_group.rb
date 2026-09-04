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
