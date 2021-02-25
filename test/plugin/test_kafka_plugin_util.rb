require 'helper'
require 'fluent/plugin/kafka_plugin_util'

class File
    def File::read(path)
        path
    end
end

class KafkaPluginUtilTest < Test::Unit::TestCase

    def self.config_param(name, type, options)
    end
    include Fluent::KafkaPluginUtil::SSLSettings

    def config_param
    end
    def setup
        Fluent::Test.setup
    end

    def test_read_ssl_file_when_nil
        assert_equal(nil, read_ssl_file(nil))
    end

    def test_read_ssl_file_when_empty_string
        assert_equal(nil, read_ssl_file(""))
    end

    def test_read_ssl_file_when_non_empty_path
        assert_equal("path", read_ssl_file("path"))
    end

    def test_read_ssl_file_when_non_empty_array
        assert_equal(["a","b"], read_ssl_file(["a","b"]))
    end

    def test_read_ssl_ca_certs_when_nil
      assert_equal(nil, read_ssl_ca_certs())
    end

    def test_read_ssl_ca_certs_general_case
      begin_cert = "-----BEGIN CERTIFICATE-----\n"
      end_cert = "\n-----END CERTIFICATE-----\n"
      cert_one = begin_cert + "one" + end_cert
      cert_two = begin_cert + "two" + end_cert
      cert_three = begin_cert + "three" + end_cert
      @ssl_ca_cert = [nil, "", cert_one, cert_two + cert_three]
      assert_equal([cert_one, cert_two, cert_three], read_ssl_ca_certs())
    end
end
