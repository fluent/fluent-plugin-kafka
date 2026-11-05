require 'helper'
require 'open3'
require 'rbconfig'

# The plugins load ActiveSupport only when active_support_notification_regex or
# monitoring_list is set, and other code in the same process may have loaded it
# already. So each test configures a plugin in a fresh Ruby process, where
# ActiveSupport is loaded only by the plugin, and then lets ruby-kafka send
# a notification as it does for every request to a broker.
class ActiveSupportTest < Test::Unit::TestCase
  LIB_DIR = File.expand_path('../../lib', __dir__)

  def have_gem(name)
    !Gem::Specification.find_all_by_name(name).empty?
  end

  def setup
    omit_unless(have_gem('activesupport'), "activesupport isn't installed")
  end

  # Returns the tags of the events that the plugin emitted.
  # With restart, it runs a second instance after the first one is shut down,
  # as a graceful reload does in the same process, and returns the tags of both.
  def run_plugin(plugin, klass, conf, restart: false)
    script = <<~RUBY
      require 'bundler/setup'
      require 'fluent/test'
      require 'fluent/test/driver/output'
      require 'fluent/plugin/#{plugin}'

      # fluent/test loads test-unit, which would run the test files at exit
      Test::Unit::AutoRunner.need_auto_run = false
      Fluent::Test.setup
      (#{restart} ? 2 : 1).times do
        d = Fluent::Test::Driver::Output.new(#{klass})
        d.configure(#{conf.dump})
        d.run do
          notification = {broker_host: 'localhost', api: :metadata, request_size: 0, response_size: 0}
          Kafka::Instrumenter.new(client_id: 'fluentd').instrument('request.connection', notification) {}
        end
        puts d.events.map(&:first)
      end
    RUBY
    stdout, stderr, status = Open3.capture3(RbConfig.ruby, '-I', LIB_DIR, '-e', script)
    assert_true(status.success?, stderr)
    stdout.lines(chomp: true)
  end

  data("kafka" => ["out_kafka", "Fluent::KafkaOutput"],
       "kafka_buffered" => ["out_kafka_buffered", "Fluent::KafkaOutputBuffered"],
       "kafka2" => ["out_kafka2", "Fluent::Kafka2Output"])
  def test_active_support_notification_regex(data)
    plugin, klass = data
    tags = run_plugin(plugin, klass, <<~CONF)
      default_topic kitagawakeiko
      brokers localhost:9092
      active_support_notification_regex kafka
      <format>
        @type json
      </format>
    CONF

    assert_equal ['fluent_kafka_stats.request.connection.kafka'], tags
  end

  data("kafka" => ["out_kafka", "Fluent::KafkaOutput"],
       "kafka_buffered" => ["out_kafka_buffered", "Fluent::KafkaOutputBuffered"],
       "kafka2" => ["out_kafka2", "Fluent::Kafka2Output"])
  def test_active_support_notification_regex_after_restart(data)
    plugin, klass = data
    tags = run_plugin(plugin, klass, <<~CONF, restart: true)
      default_topic kitagawakeiko
      brokers localhost:9092
      active_support_notification_regex kafka
      <format>
        @type json
      </format>
    CONF

    assert_equal ['fluent_kafka_stats.request.connection.kafka'] * 2, tags
  end

  data("kafka_buffered" => ["out_kafka_buffered", "Fluent::KafkaOutputBuffered"],
       "kafka2" => ["out_kafka2", "Fluent::Kafka2Output"])
  def test_monitoring_list(data)
    omit_unless(have_gem('statsd-ruby'), "statsd-ruby isn't installed")
    plugin, klass = data
    tags = run_plugin(plugin, klass, <<~CONF)
      default_topic kitagawakeiko
      brokers localhost:9092
      monitoring_list statsd
      <format>
        @type json
      </format>
    CONF

    assert_equal [], tags
  end
end
