require 'helper'
require 'fluent/test/helpers'
require 'fluent/test/driver/input'
require 'fluent/test/driver/output'
require 'securerandom'

class Rdkafka2OutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def have_rdkafka
    begin
      require 'fluent/plugin/out_rdkafka2'
      true
    rescue LoadError
      false
    end
  end

  def setup
    omit_unless(have_rdkafka, "rdkafka isn't installed")
    Fluent::Test.setup
  end

  def base_config
    config_element('ROOT', '', {"@type" => "rdkafka2"}, [
                     config_element('format', "", {"@type" => "json"})
                   ])
  end

  def config(default_topic: "kitagawakeiko")
    base_config + config_element('ROOT', '', {"default_topic" => default_topic,
                                              "brokers" => "localhost:9092"}, [
                                 ])
  end

  def create_driver(conf = config, tag='test')
    Fluent::Test::Driver::Output.new(Fluent::Rdkafka2Output).configure(conf)
  end

  def test_configure
    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(base_config)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(config)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(config + config_element('buffer', "", {"@type" => "memory"}))
    }

    d = create_driver
    assert_equal 'kitagawakeiko', d.instance.default_topic
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_configure_sasl_plain_over_ssl
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass",
                                                     "ssl_ca_cert" => "/path/to/ca_cert.pem"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'PLAIN', config[:"sasl.mechanisms"]
    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'testuser', config[:"sasl.username"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  def test_configure_sasl_plain_without_ssl
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass"}, [])

    assert_raise(Fluent::ConfigError) {
      create_driver(conf)
    }
  end

  def test_configure_sasl_plain_without_ssl_allowed_by_sasl_over_ssl
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass",
                                                     "sasl_over_ssl" => "false"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'PLAIN', config[:"sasl.mechanisms"]
    assert_equal 'SASL_PLAINTEXT', config[:"security.protocol"]
    assert_equal 'testuser', config[:"sasl.username"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  def test_configure_sasl_plain_with_security_protocol_from_rdkafka_options
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass",
                                                     "rdkafka_options" => '{"security.protocol": "SASL_SSL"}'}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
  end

  data("sha256" => ["sha256", "SCRAM-SHA-256"],
       "sha512" => ["sha512", "SCRAM-SHA-512"])
  def test_configure_sasl_scram(data)
    mechanism, expected = data
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass",
                                                     "scram_mechanism" => mechanism,
                                                     "ssl_ca_cert" => "/path/to/ca_cert.pem"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal expected, config[:"sasl.mechanisms"]
    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'testuser', config[:"sasl.username"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  def test_configure_ssl_ca_certs_from_system
    conf = base_config + config_element('ROOT', '', {"ssl_ca_certs_from_system" => "true"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_nil config[:"ssl.ca.location"]
  end

  def test_configure_ssl_client_cert_without_ca_cert
    conf = base_config + config_element('ROOT', '', {"ssl_client_cert" => "/path/to/cert.pem",
                                                     "ssl_client_cert_key" => "/path/to/key.pem"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal '/path/to/cert.pem', config[:"ssl.certificate.location"]
    assert_equal '/path/to/key.pem', config[:"ssl.key.location"]
    assert_nil config[:"ssl.ca.location"]
  end

  def test_configure_sasl_plain_over_ssl_ca_certs_from_system
    conf = base_config + config_element('ROOT', '', {"username" => "testuser", "password" => "testpass",
                                                     "ssl_ca_certs_from_system" => "true"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'testpass', config[:"sasl.password"]
  end

  def test_configure_ssl_verify_hostname_default
    conf = base_config + config_element('ROOT', '', {"ssl_ca_cert" => "/path/to/ca_cert.pem"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal 'https', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
  end

  def test_configure_ssl_verify_hostname_false
    conf = base_config + config_element('ROOT', '', {"ssl_ca_cert" => "/path/to/ca_cert.pem",
                                                     "ssl_verify_hostname" => "false"}, [])
    d = create_driver(conf)

    config = d.instance.build_config

    assert_equal 'none', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
  end

  def test_configure_without_ssl_has_no_endpoint_identification
    d = create_driver

    config = d.instance.build_config

    assert_equal 'PLAINTEXT', config[:"security.protocol"]
    assert_nil config[:"ssl.endpoint.identification.algorithm"]
    assert_nil config[:"enable.ssl.certificate.verification"]
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?
  end

  class WriteTest < self
    TOPIC_NAME = "kafka-output-#{SecureRandom.uuid}"

    INPUT_CONFIG = %[
      @type kafka
      brokers localhost:9092
      format json
      @label @kafka
      topics #{TOPIC_NAME}
    ]

    def create_target_driver(conf = INPUT_CONFIG)
      Fluent::Test::Driver::Input.new(Fluent::KafkaInput).configure(conf)
    end

    def setup
      @kafka = nil
      omit_unless(have_rdkafka, "rdkafka isn't installed")
      @kafka = Kafka.new(["localhost:9092"], client_id: 'kafka')
    end

    def teardown
      if @kafka
        @kafka.delete_topic(TOPIC_NAME)
        @kafka.close
      end
    end

    def test_write
      target_driver = create_target_driver
      expected_message = {"a" => 2}
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(config(default_topic: TOPIC_NAME))
        d.run do
          d.feed("test", event_time, expected_message)
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([expected_message], actual_messages)
    end

    def test_write_with_use_event_time
      input_config = %[
        @type kafka
        brokers localhost:9092
        format json
        @label @kafka
        topics #{TOPIC_NAME}
        time_source kafka
      ]
      target_driver = create_target_driver(input_config)
      expected_message = {"a" => 2}
      now = event_time
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(config(default_topic: TOPIC_NAME) + config_element('ROOT', '', {"use_event_time" => true}))
        d.run do
          d.feed("test", now, expected_message)
        end
      end
      actual_time = target_driver.events.collect { |event| event[1] }.last
      assert_in_delta(actual_time, now, 0.001) # expects millseconds precision
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([expected_message], actual_messages)
    end

    def test_exclude_fields
      conf = config(default_topic: TOPIC_NAME) +
             config_element('ROOT', '', {"exclude_fields" => "$.foo"}, [])
      target_driver = create_target_driver
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(conf)
        d.run do
          d.feed('test', event_time, {'a' => 'b', 'foo' => 'bar', 'message' => 'test'})
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([{'a' => 'b', 'message' => 'test'}], actual_messages)
    end

    def test_max_enqueue_bytes_per_second
      conf = config(default_topic: TOPIC_NAME) +
             config_element('ROOT', '', {"max_enqueue_bytes_per_second" => 32 * 3}, [])
      target_driver = create_target_driver
      expected_messages = []
      target_driver.run(expect_records: 9, timeout: 10) do
        sleep 2
        d = create_driver(conf)
        start_time = Fluent::Clock.now
        d.run do
          9.times do |i|
            message = {"message" => "32bytes message: #{i}"}
            d.feed("test", event_time, message)
            expected_messages << message
          end
        end
        assert_in_delta(2.0, Fluent::Clock.now - start_time, 0.5)
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal(expected_messages, actual_messages)
    end

    def test_record_key
      conf = config(default_topic: TOPIC_NAME) +
             config_element('ROOT', '', {"record_key" => "$.data"}, [])
      target_driver = create_target_driver
      target_driver.run(expect_records: 1, timeout: 5) do
        sleep 2
        d = create_driver(conf)
        d.run do
          d.feed('test', event_time, {'data' => {'a' => 'b', 'foo' => 'bar', 'message' => 'test'}, 'message_key' => '123456'})
        end
      end
      actual_messages = target_driver.events.collect { |event| event[2] }
      assert_equal([{'a' => 'b', 'foo' => 'bar', 'message' => 'test'}], actual_messages)
    end
  end
end
