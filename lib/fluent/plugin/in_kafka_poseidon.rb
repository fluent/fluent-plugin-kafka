module Fluent

class KafkaPoseidonInput < Input
  Plugin.register_input('kafka-poseidon', self)

  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 2181
  config_param :interval, :integer, :default => 1 # seconds
  config_param :topics, :string
  config_param :client_id, :string, :default => 'kafka-poseidon'
  config_param :partition, :integer, :default => 0
  config_param :offset, :integer, :default => -1

  def initialize
    super
    require 'poseidon'
  end

  def configure(conf)
    super
    @topic_list = @topics.split(',').map {|topic| topic.strip }
    if @topic_list.empty?
      raise ConfigError, "kafka: 'topics' is a require parameter"
    end
  end

  def start
    @loop = Coolio::Loop.new
    @topic_watchers = @topic_list.map {|topic|
      TopicWatcher.new(topic, @host, @port, @client_id, @partition, @offset, interval)
    }
    @topic_watchers.each {|tw|
      tw.attach(@loop)
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  class TopicWatcher < Coolio::TimerWatcher
    def initialize(topic, host, port, client_id, partition, offset, interval)
      @topic = topic
      @callback = method(:consume)
      @consumer = Poseidon::PartitionConsumer.new(
        client_id,            # client_id
        host,                 # host
        port,                 # port
        topic,                # topic
        partition,            # partition
        offset                # offset
      )
        
      super(interval, true)
    end

    def on_timer
      @callback.call
    rescue
        # TODO log?
        $log.error $!.to_s
        $log.error_backtrace
    end

    def consume
      es = MultiEventStream.new
      @consumer.fetch.each { |msg|
        begin
          msg_record = {
            :topic  => msg.topic,
            :value  => msg.value,
            :key    => msg.key,
            :offset => msg.offset
         }
          es.add(Time.now.to_i, msg_record)
        rescue
          $log.warn msg_record.to_s, :error=>$!.to_s
          $log.debug_backtrace
        end
      }

      unless es.empty?
        Engine.emit_stream(@topic, es)
      end
    end
  end
end

end
