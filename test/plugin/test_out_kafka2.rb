require 'helper'
require 'fluent/plugin/out_kafka2'
require 'fluent/test/helpers'
require 'fluent/test/driver/output'

class Kafka2OutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  BASE_CONFIG = %[
    type kafka2
    <format>
      @type json
    </format>
  ]

  CONFIG = BASE_CONFIG + %[
    default_topic test
    brokers localhost:9092
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Kafka2Output).configure(conf)
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
    assert_equal('test', d.instance.default_topic)
    assert_equal(['localhost:9092'], d.instance.brokers)
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
    time = event_time("2011-01-02 13:14:15 UTC")
    d.run(default_tag: 'test') do
      d.feed(time, {"a" => 1})
      d.feed('test', time, {"a" => 2})
    end
  end
end
