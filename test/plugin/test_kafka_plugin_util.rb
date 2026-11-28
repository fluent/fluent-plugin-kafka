require 'helper'
require 'fluent/plugin/kafka_plugin_util'
require 'openssl'
require 'tmpdir'

class KafkaPluginUtilTest < Test::Unit::TestCase

    def self.config_param(name, type, options)
    end
    include Fluent::KafkaPluginUtil::SSLSettings
    include Fluent::KafkaPluginUtil::PartitionSettings
    include Fluent::KafkaPluginUtil::MessageEncoding

    def config_param
    end
    def setup
        Fluent::Test.setup
    end

    data("integer" => [3, 3],
         "decimal string" => ["3", 3],
         "zero padded string" => ["010", 10],
         "unassigned partition" => [-1, -1],
         "max int32" => [2**31 - 1, 2**31 - 1])
    def test_coerce_partition(data)
      given, expected = data
      assert_equal(expected, coerce_partition(given))
    end

    data("non numeric string" => ["not-a-number", ArgumentError],
         "empty string" => ["", ArgumentError],
         "hexadecimal string" => ["0x10", ArgumentError],
         "float string" => ["3.5", ArgumentError],
         "float" => [3.9, TypeError],
         "nil" => [nil, TypeError],
         "negative partition" => [-2, RangeError],
         "too big for int32" => [2**31, RangeError])
    def test_coerce_partition_rejects_invalid_value(data)
      given, expected = data
      assert_raise(expected) do
        coerce_partition(given)
      end
    end

    def test_read_ssl_file_when_nil
      stub(File).read(anything) do |path|
        path
      end
      assert_equal(nil, read_ssl_file(nil))
    end

    def test_read_ssl_file_when_empty_string
      stub(File).read(anything) do |path|
        path
      end
      assert_equal(nil, read_ssl_file(""))
    end

    def test_read_ssl_file_when_non_empty_path
      stub(File).read(anything) do |path|
        path
      end
      assert_equal("path", read_ssl_file("path"))
    end

    def test_read_ssl_file_when_non_empty_array
      stub(File).read(anything) do |path|
        path
      end
      assert_equal(["a","b"], read_ssl_file(["a","b"]))
    end

    def create_certificate(common_name, key)
      cert = OpenSSL::X509::Certificate.new
      cert.version = 2
      cert.serial = 1
      cert.subject = cert.issuer = OpenSSL::X509::Name.parse("/CN=#{common_name}")
      cert.public_key = key
      cert.not_before = Time.now
      cert.not_after = Time.now + 3600
      cert.sign(key, OpenSSL::Digest.new('SHA256'))
      cert
    end

    data("only certificates" => ->(certs, key) { certs.map(&:to_pem).join },
         "blank line at the end" => ->(certs, key) { certs.map(&:to_pem).join + "\n" },
         "private key after the certificates" => ->(certs, key) { certs.map(&:to_pem).join + key.private_to_pem },
         "X509 CERTIFICATE label on the last certificate" => ->(certs, key) {
           certs[0].to_pem + certs[1].to_pem.gsub("CERTIFICATE", "X509 CERTIFICATE")
         },
         "non-UTF-8 text before the certificates" => ->(certs, key) {
           "Bag Attributes\n    friendlyName: M\xFCller\n".b + certs.map(&:to_pem).join
         })
    def test_ruby_kafka_ssl_params_with_client_cert_chain(build_chain)
      key = OpenSSL::PKey::EC.generate('prime256v1')
      client_cert = create_certificate('client', key)
      chain = [create_certificate('intermediate1', key), create_certificate('intermediate2', key)]

      Dir.mktmpdir do |dir|
        @ssl_client_cert = File.join(dir, 'client_cert.pem')
        @ssl_client_cert_key = File.join(dir, 'client_cert_key.pem')
        @ssl_client_cert_chain = File.join(dir, 'client_cert_chain.pem')
        @ssl_verify_hostname = true
        File.write(@ssl_client_cert, client_cert.to_pem)
        File.write(@ssl_client_cert_key, key.private_to_pem)
        File.write(@ssl_client_cert_chain, build_chain.call(chain, key))

        kafka = Kafka.new(['localhost:9092'], **ruby_kafka_ssl_params)
        ssl_context = kafka.instance_variable_get(:@connection_builder).instance_variable_get(:@ssl_context)

        assert_equal(chain.map(&:to_der), ssl_context.extra_chain_cert.map(&:to_der))
      end
    end

    data("valid UTF-8" => ["あ".b, "あ"],
         "frozen" => ["あ".b.freeze, "あ"],
         "ASCII only" => ["abc".b, "abc".b],
         "invalid UTF-8" => ["\xFF".b, "\xFF".b],
         "already UTF-8" => ["あ", "あ"],
         "nil" => [nil, nil])
    def test_as_utf8(data)
      given, expected = data
      actual = as_utf8(given)
      assert_equal([expected, expected&.encoding], [actual, actual&.encoding])
    end

    def test_as_utf8_keeps_argument
      str = "あ".b
      as_utf8(str)
      assert_equal(Encoding::ASCII_8BIT, str.encoding)
    end

    # The default format of out_file writes each record with JSON.generate
    def test_unrecoverable_errors_include_error_for_deeply_nested_record
      record = (1..101).inject("value") { |value, _| {"nested" => value} }
      assert_raise(*Fluent::KafkaPluginUtil::EmitErrors::UNRECOVERABLE_ERRORS) do
        JSON.generate(record)
      end
    end

end
