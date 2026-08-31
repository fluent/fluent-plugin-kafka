require 'helper'
require 'fluent/plugin/kafka_plugin_util'

class KafkaPluginUtilTest < Test::Unit::TestCase

    def self.config_param(name, type, options)
    end
    include Fluent::KafkaPluginUtil::SSLSettings
    include Fluent::KafkaPluginUtil::PartitionSettings

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

end
