require 'helper'
require 'fluent/test/helpers'
require 'fluent/test/driver/output'

class Kafka2OutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  def base_config
    config_element('ROOT', '', {"@type" => "kafka2"}, [
                     config_element('format', "", {"@type" => "json"})
                   ])
  end

  def config
    base_config + config_element('ROOT', '', {"default_topic" => "kitagawakeiko",
                                              "brokers" => "localhost:9092"}, [
                                 ])
  end

  def create_driver(conf = config, tag='test')
    Fluent::Test::Driver::Output.new(Fluent::Kafka2Output).configure(conf)
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
    assert_equal ['localhost:9092'], d.instance.brokers
  end

  data("crc32" => "crc32",
       "murmur2" => "murmur2")
  def test_partitioner_hash_function(data)
    hash_type = data
    d = create_driver(config + config_element('ROOT', '', {"partitioner_hash_function" => hash_type}))
    assert_nothing_raised do
      d.instance.refresh_client
    end
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?
  end

  def test_exclude_fields
    d = create_driver(config + config_element('ROOT', '', {"exclude_fields" => "$.foo"}))
    d.run do
      d.feed('test', event_time, {'a' => 'b', 'foo' => 'bar', 'message' => 'test'})
    end

    assert_equal(1, d.events.size)
  end
end
