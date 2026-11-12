require 'helper'
require 'timeout'

class RdkafkaProducerShutdownTest < Test::Unit::TestCase
  def setup
    begin
      require 'fluent/plugin/out_rdkafka2'
    rescue LoadError
      omit "rdkafka isn't installed"
    end
    version = Gem::Version.create(Rdkafka::VERSION)
    omit_unless(version >= Gem::Version.create('0.8.0') && version < Gem::Version.create('0.12.0'),
                'this checks the patch for rdkafka 0.8.0 to 0.11.x')
  end

  def with_producer
    producer = Rdkafka::Config.new('bootstrap.servers' => '127.0.0.1:1',
                                   'message.timeout.ms' => 1000).producer
    yield producer
  ensure
    producer.close(10) if producer
  end

  def test_close_waits_for_a_produce_that_passed_the_closed_check
    with_producer do |producer|
      entered = Queue.new
      resume = Queue.new
      close_started = Queue.new
      original_check = producer.method(:closed_producer_check)
      stub(producer).closed_producer_check do |method|
        original_check.call(method)
        entered << true
        resume.pop
      end

      # Turn a regression into a Ruby exception rather than a native crash.
      original_produce = Rdkafka::Bindings.method(:rd_kafka_producev)
      stub(Rdkafka::Bindings).rd_kafka_producev do |native, *args|
        raise 'NULL producer handle' unless native
        original_produce.call(native, *args)
      end

      writer = Thread.new { producer.produce(topic: 'close-race', payload: 'message') }
      writer.report_on_exception = false
      writer.abort_on_exception = false
      closer = nil
      begin
        Timeout.timeout(10) do
          entered.pop
          closer = Thread.new do
            close_started << true
            producer.close(10)
          end
          close_started.pop
          Thread.pass while closer.status == 'run'

          assert_equal 'sleep', closer.status
          assert_false producer.instance_variable_get(:@closing)
          assert_not_nil producer.instance_variable_get(:@native_kafka)

          resume << true
          assert_kind_of Rdkafka::Producer::DeliveryHandle, writer.value
          assert_true closer.value
          assert_nil producer.instance_variable_get(:@native_kafka)
        end
      ensure
        resume << true
        writer.join rescue nil
        closer.join if closer
      end
    end
  end

  def test_produce_is_rejected_after_polling_stops_before_destroy
    with_producer do |producer|
      polling_thread = producer.instance_variable_get(:@polling_thread)
      original_destroy = Rdkafka::Bindings.method(:rd_kafka_destroy)
      rejected = false
      polling_stopped = false
      stub(Rdkafka::Bindings).rd_kafka_destroy do |native|
        polling_stopped = !polling_thread.alive?
        begin
          producer.produce(topic: 'close-race', payload: 'too late')
        rescue Rdkafka::ClosedProducerError
          rejected = true
        end
        original_destroy.call(native)
      end

      assert_true producer.close(10)
      assert_true polling_stopped
      assert_true rejected
    end
  end
end
