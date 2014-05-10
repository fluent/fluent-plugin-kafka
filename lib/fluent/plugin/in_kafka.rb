module Fluent

class KafkaInput < Input
  Plugin.register_input('kafka', self)

  config_param :format, :string, :default => 'json' # (json|text)
  config_param :host, :string, :default => 'localhost'
  config_param :port, :integer, :default => 2181
  config_param :interval, :integer, :default => 1 # seconds
  config_param :topics, :string
  config_param :client_id, :string, :default => 'kafka'
  config_param :partition, :integer, :default => 0
  config_param :offset, :integer, :default => -1
  config_param :add_prefix, :string, :default => nil
  config_param :add_suffix, :string, :default => nil

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

    case @format
    when 'json'
      require 'yajl'
    end
  end

  def start
    @loop = Coolio::Loop.new
    @topic_watchers = @topic_list.map {|topic|
      TopicWatcher.new(topic, @host, @port, @client_id, @partition, @offset, interval, @format, @add_prefix, @add_suffix)
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
    def initialize(topic, host, port, client_id, partition, offset, interval, format, add_prefix, add_suffix)
      @topic = topic
      @callback = method(:consume)
      @format = format
      @add_prefix = add_prefix
      @add_suffix = add_suffix
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
      tag = @topic
      tag = @add_prefix + "." + tag if @add_prefix
      tag = tag + "." + @add_suffix if @add_suffix
      @consumer.fetch.each { |msg|
        begin
          msg_record = parse_line(msg.value)
          es.add(Time.now.to_i, msg_record)
        rescue
          $log.warn msg_record.to_s, :error=>$!.to_s
          $log.debug_backtrace
        end
      }

      unless es.empty?
        Engine.emit_stream(tag, es)
      end
    end

    def parse_line(record)
      parsed_record = {}
      case @format
      when 'json'
        parsed_record = Yajl::Parser.parse(record)
      when 'text'
        parsed_record = record
      end
      parsed_record
    end
  end
end

end
