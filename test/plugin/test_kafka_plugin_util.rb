require 'helper'
require 'fluent/plugin/kafka_plugin_util'

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
