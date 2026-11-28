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
    include RubyKafkaSSLParamsTestHelper

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

    data("only certificates" => ->(certs, key) { certs.map(&:to_pem).join },
         "blank line at the end" => ->(certs, key) { certs.map(&:to_pem).join + "\n" },
         "private key after the certificates" => ->(certs, key) { certs.map(&:to_pem).join + key.private_to_pem },
         "private key before the certificates" => ->(certs, key) { key.private_to_pem + certs.map(&:to_pem).join },
         "no newline at the end" => ->(certs, key) { certs.map(&:to_pem).join.chomp },
         "X509 CERTIFICATE label on the first certificate" => ->(certs, key) {
           certs[0].to_pem.gsub("CERTIFICATE", "X509 CERTIFICATE") + certs[1].to_pem
         },
         "X509 CERTIFICATE label on the last certificate" => ->(certs, key) {
           certs[0].to_pem + certs[1].to_pem.gsub("CERTIFICATE", "X509 CERTIFICATE")
         },
         "CRLF line endings" => ->(certs, key) { certs.map(&:to_pem).join.gsub("\n", "\r\n") },
         "CRLF line endings and private key after the certificates" => ->(certs, key) {
           (certs.map(&:to_pem).join + key.private_to_pem).gsub("\n", "\r\n")
         },
         "non-UTF-8 text before the certificates" => ->(certs, key) {
           "Bag Attributes\n    friendlyName: M\xFCller\n".b + certs.map(&:to_pem).join
         })
    def test_ruby_kafka_ssl_params_with_client_cert_chain(build_chain)
      key = OpenSSL::PKey::EC.generate('prime256v1')
      chain = [create_certificate('intermediate1', key), create_certificate('intermediate2', key)]

      Dir.mktmpdir do |dir|
        ssl_context = build_ruby_kafka_ssl_context(dir, key, build_chain.call(chain, key))

        assert_equal(chain.map(&:to_der), ssl_context.extra_chain_cert.map(&:to_der))
      end
    end

    def test_ruby_kafka_ssl_params_with_empty_client_cert_chain
      key = OpenSSL::PKey::EC.generate('prime256v1')

      Dir.mktmpdir do |dir|
        ssl_context = build_ruby_kafka_ssl_context(dir, key, "")

        assert_equal([], ssl_context.extra_chain_cert)
      end
    end

    data("first certificate" => [0],
         "last certificate" => [1],
         "all certificates" => [0, 1])
    def test_ruby_kafka_ssl_params_rejects_trusted_certificate_in_chain(trusted)
      key = OpenSSL::PKey::EC.generate('prime256v1')
      chain = [create_certificate('intermediate1', key), create_certificate('intermediate2', key)]
      chain_pem = chain.each_with_index.map { |cert, i|
        pem = cert.to_pem
        trusted.include?(i) ? pem.gsub("CERTIFICATE", "TRUSTED CERTIFICATE") : pem
      }.join

      Dir.mktmpdir do |dir|
        error = assert_raise(Fluent::ConfigError) do
          build_ruby_kafka_ssl_context(dir, key, chain_pem)
        end
        assert_match(/TRUSTED CERTIFICATE label/, error.message)
      end
    end

    # Passes ruby_kafka_ssl_params to ruby-kafka and returns the SSL context it builds
    def build_ruby_kafka_ssl_context(dir, key, chain)
      @ssl_client_cert = File.join(dir, 'client_cert.pem')
      @ssl_client_cert_key = File.join(dir, 'client_cert_key.pem')
      @ssl_client_cert_chain = File.join(dir, 'client_cert_chain.pem')
      @ssl_verify_hostname = true
      File.write(@ssl_client_cert, create_certificate('client', key).to_pem)
      File.write(@ssl_client_cert_key, key.private_to_pem)
      File.binwrite(@ssl_client_cert_chain, chain)

      kafka = Kafka.new(['localhost:9092'], **ruby_kafka_ssl_params)
      kafka.instance_variable_get(:@connection_builder).instance_variable_get(:@ssl_context)
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

end
