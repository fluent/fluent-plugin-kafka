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
end
