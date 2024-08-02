require 'fluent/test'
require 'fluent/test/helpers'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_waterdrop'
require 'rdkafka'
require 'json'

# 1. run docker-compose to spin up the Kafka broker
# 2. Run these tests
class WaterdropOutputTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  def create_driver(conf, tag = 'test')
    Fluent::Test::Driver::Output.new(Fluent::WaterdropOutput).configure(conf)
  end

  sub_test_case 'configure' do
    test 'basic configuration' do
      assert_nothing_raised(Fluent::ConfigError) do
        config = %[
          @type waterdrop
          <format>
            @type json
          </format>
        ]
        driver = create_driver(config)

        assert_equal 'localhost:9092', driver.instance.bootstrap_servers
      end
    end

    test 'missing format section' do
      assert_raise(Fluent::ConfigError) do
        config = %[
          @type waterdrop
        ]
        create_driver(config)
      end
    end

    test 'formatter section missing @type' do
      assert_raise(Fluent::ConfigError) do
        config = %[
          @type waterdrop
          <format>
            literally 'anything else'
          </format>
        ]
        create_driver(config)
      end
    end
  end

  sub_test_case 'produce' do
    GLOBAL_CONFIG = {
      "bootstrap.servers" => "localhost:9092",
      "topic.metadata.propagation.max.ms" => 11 * 1_000,
      "topic.metadata.refresh.interval.ms" => 10 * 1_000,
    }
    TOPIC = 'produce.basic-produce'

    def setup
      @kafka_admin = Rdkafka::Config.new(GLOBAL_CONFIG).admin
      @kafka_consumer = Rdkafka::Config.new(
        GLOBAL_CONFIG.merge(
          {
            "group.id" => "waterdrop",
            "auto.offset.reset" => "earliest",
          }
        )
      ).consumer

      @kafka_admin.delete_topic(TOPIC)
      @kafka_admin.create_topic(TOPIC, 1, 1)
                  .wait(max_wait_timeout: 30)
      @kafka_consumer.subscribe(TOPIC)
    end

    def teardown
      @kafka_consumer.close
      @kafka_admin.delete_topic(TOPIC)
      @kafka_admin.close
    end

    test 'basic produce' do
      config = %[
          @type waterdrop
          default_topic #{TOPIC}
          <format>
            @type json
          </format>
        ]
      d = create_driver(config)
      d.run(default_tag: TOPIC, flush: true) do
        d.feed(Fluent::EventTime.now, { topic: TOPIC, body: '123' })
      end

      sleep(12)

      raw_message = @kafka_consumer.poll(5_000)

      message = JSON.parse!(raw_message.payload)
      assert_equal '123', message["body"]
    end
  end
end