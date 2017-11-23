require 'helper'
require 'fluent/output'

class KafkaOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  BASE_CONFIG = %[
    type kafka_buffered
  ]

  CONFIG = BASE_CONFIG + %[
    default_topic kitagawakeiko
    brokers localhost:9092
  ]

  def create_driver(conf = CONFIG, tag='test')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::KafkaOutput, tag).configure(conf)
  end

  def test_configure
    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(BASE_CONFIG)
    }
    
    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(CONFIG)
    }

    assert_nothing_raised(Fluent::ConfigError) {
      create_driver(CONFIG + %[
        buffer_type memory
      ])
    }

    d = create_driver
    assert_equal 'kitagawakeiko', d.instance.default_topic
    assert_equal 'localhost:9092', d.instance.brokers
  end

  def test_format
    d = create_driver
  end

  def test_mutli_worker_support
    d = create_driver
    assert_equal true, d.instance.multi_workers_ready?

  end

  def test_write
    d = create_driver
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
  end
end
