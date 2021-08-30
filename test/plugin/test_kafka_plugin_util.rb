require 'helper'
require 'fluent/plugin/kafka_plugin_util'

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
