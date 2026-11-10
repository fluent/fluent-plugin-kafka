require 'helper'
require 'fluent/test/helpers'
require 'fluent/test/driver/output'

class RdkafkaOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def have_rdkafka
    begin
      require 'fluent/plugin/out_rdkafka'
      true
    rescue LoadError
      false
    end
  end

  def setup
    omit_unless(have_rdkafka, "rdkafka isn't installed")
    Fluent::Test.setup
  end

  def base_config(params = {})
    config_element('ROOT', '', {"@type" => "rdkafka",
                                "brokers" => "localhost:9092"}.merge(params), [])
  end

  def create_driver(conf = base_config)
    Fluent::Test::Driver::Output.new(Fluent::KafkaOutputBuffered2).configure(conf)
  end

  def test_configure
    d = create_driver

    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_configure_ssl_ca_cert
    d = create_driver(base_config("ssl_ca_cert" => "/path/to/ca_cert.pem"))

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal '/path/to/ca_cert.pem', config[:"ssl.ca.location"]
  end

  def test_configure_ssl_ca_certs_from_system
    d = create_driver(base_config("ssl_ca_certs_from_system" => "true"))

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_nil config[:"ssl.ca.location"]
  end

  def test_configure_ssl_client_cert_without_ca_cert
    d = create_driver(base_config("ssl_client_cert" => "/path/to/cert.pem",
                                  "ssl_client_cert_key" => "/path/to/key.pem"))

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal '/path/to/cert.pem', config[:"ssl.certificate.location"]
    assert_equal '/path/to/key.pem', config[:"ssl.key.location"]
    assert_nil config[:"ssl.ca.location"]
  end

  def test_configure_ssl_verify_hostname_default
    d = create_driver(base_config("ssl_ca_cert" => "/path/to/ca_cert.pem"))

    config = d.instance.build_config

    assert_equal 'SSL', config[:"security.protocol"]
    assert_equal 'https', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
  end

  def test_configure_ssl_verify_hostname_false
    d = create_driver(base_config("ssl_ca_cert" => "/path/to/ca_cert.pem",
                                  "ssl_verify_hostname" => "false"))

    config = d.instance.build_config

    assert_equal 'none', config[:"ssl.endpoint.identification.algorithm"]
    assert_equal true, config[:"enable.ssl.certificate.verification"]
  end

  def test_configure_without_ssl_has_no_endpoint_identification
    config = create_driver.instance.build_config

    assert_equal 'PLAINTEXT', config[:"security.protocol"]
    assert_nil config[:"ssl.endpoint.identification.algorithm"]
    assert_nil config[:"enable.ssl.certificate.verification"]
  end

  def test_configure_sasl_gssapi_over_ssl
    d = create_driver(base_config("principal" => "testuser@EXAMPLE.COM",
                                  "ssl_client_cert" => "/path/to/cert.pem",
                                  "ssl_client_cert_key" => "/path/to/key.pem"))

    config = d.instance.build_config

    assert_equal 'SASL_SSL', config[:"security.protocol"]
    assert_equal 'GSSAPI', config[:"sasl.mechanisms"]
  end

  class DummyDeliveryHandle
    attr_reader :waited

    def wait(**)
      @waited = true
    end
  end

  class DummyProducer
    attr_reader :produced, :handles

    def initialize
      @produced = []
      @handles = []
    end

    # Raises what the rdkafka FFI binding raises: the partition must be an
    # int32 and the key must respond to #bytesize
    def produce(topic:, payload:, key:, partition:, timestamp:)
      unless partition.nil?
        raise TypeError, "no implicit conversion of #{partition.class} into Integer" unless partition.is_a?(Integer)
        raise RangeError, "integer #{partition} too big to convert to 'int'" unless (-2**31..2**31 - 1).cover?(partition)
      end
      key.bytesize unless key.nil?

      @produced << {payload: payload, key: key, partition: partition}
      @handles << DummyDeliveryHandle.new
      @handles.last
    end
  end

  def write_records(records, params = {})
    d = create_driver(base_config({"default_topic" => "kitagawakeiko"}.merge(params)))
    producer = DummyProducer.new
    stub(d.instance).get_producer { producer }
    metadata = Fluent::Plugin::Buffer::Metadata.new(nil, nil, {key: 'test'})
    chunk = Fluent::Plugin::Buffer::MemoryChunk.new(metadata)
    chunk.append(records.map { |record| [event_time, record].to_msgpack })
    d.instance.write(chunk)
    producer
  end

  def test_write_skips_event_over_max_send_limit_bytes
    producer = nil
    assert_nothing_raised {
      producer = write_records([{"a" => "b"}, {"a" => "c" * 100}, {"a" => "d"}], "max_send_limit_bytes" => "50")
    }

    assert_equal ['{"a":"b"}', '{"a":"d"}'], producer.produced.collect { |message| message[:payload] }
    assert_equal [true, true], producer.handles.collect(&:waited)
  end

  data("non numeric partition" => "not-a-number",
       "hexadecimal partition" => "0x10",
       "float partition" => 3.9,
       "negative partition" => -2,
       "out of int32 range" => 2**31)
  def test_write_skips_event_with_invalid_partition(partition)
    producer = nil
    assert_nothing_raised {
      producer = write_records([{"a" => "b"}, {"a" => "c", "partition" => partition}, {"a" => "d"}])
    }

    assert_equal ['{"a":"b"}', '{"a":"d"}'], producer.produced.collect { |message| message[:payload] }
  end

  def test_write_keeps_event_with_zero_padded_partition
    producer = write_records([{"a" => "b", "partition" => "010"}])

    assert_equal [10], producer.produced.collect { |message| message[:partition] }
  end

  def test_write_converts_non_string_message_key
    producer = write_records([{"a" => "b", "message_key" => 12345}])

    assert_equal ["12345"], producer.produced.collect { |message| message[:key] }
  end

  def test_write_does_not_wait_when_poll_timeout_is_zero
    producer = write_records([{"a" => "b"}], "rdkafka_delivery_handle_poll_timeout" => "0")

    assert_equal [nil], producer.handles.collect(&:waited)
  end
end
