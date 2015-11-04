module Fluent

class KafkaInput < Input
  Plugin.register_input('kafka_group', self)

  config_param :brokers, :string
  config_param :zookeepers, :string
  config_param :consumer_group, :string, :default => nil
  config_param :topics, :string
  config_param :interval, :integer, :default => 1 # seconds
  config_param :format, :string, :default => 'json' # (json|text|ltsv)
  config_param :message_key, :string, :default => 'message' # for 'text' format only
  config_param :add_prefix, :string, :default => nil
  config_param :add_suffix, :string, :default => nil

  # poseidon PartitionConsumer options
  config_param :max_bytes, :integer, :default => nil
  config_param :max_wait_ms, :integer, :default => nil
  config_param :min_bytes, :integer, :default => nil
  config_param :socket_timeout_ms, :integer, :default => nil

  def initialize
    super
    require 'poseidon_cluster'
  end

  def _config_to_array(config)
    config_array = config.split(',').map {|k| k.strip }
    if config_array.empty?
      raise ConfigError, "kafka_group: '#{config}' is a required parameter"
    end
    config_array
  end

  private :_config_to_array

  def configure(conf)
    super
    @broker_list = _config_to_array(@brokers)
    @zookeeper_list = _config_to_array(@zookeepers)
    @topic_list = _config_to_array(@topics)

    unless @consumer_group
      raise ConfigError, "kafka_group: 'consumer_group' is a required parameter"
    end
    $log.info "Will watch for topics #{@topic_list} at brokers " \
              "#{@broker_list}, zookeepers #{@zookeeper_list} and group " \
              "'#{@consumer_group}'"

    case @format
    when 'json'
      require 'yajl'
    when 'ltsv'
      require 'ltsv'
    when 'msgpack'
      require 'msgpack'
    end
  end

  def start
    @loop = Coolio::Loop.new
    opt = {}
    opt[:max_bytes] = @max_bytes if @max_bytes
    opt[:max_wait_ms] = @max_wait_ms if @max_wait_ms
    opt[:min_bytes] = @min_bytes if @min_bytes
    opt[:socket_timeout_ms] = @socket_timeout_ms if @socket_timeout_ms

    @topic_watchers = @topic_list.map {|topic|
      TopicWatcher.new(topic, @broker_list, @zookeeper_list, @consumer_group,
                       interval, @format, @message_key, @add_prefix,
                       @add_suffix, opt)
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
    def initialize(topic, broker_list, zookeeper_list, consumer_group,
                   interval, format, message_key, add_prefix, add_suffix,
                   options)
      @topic = topic
      @callback = method(:consume)
      @format = format
      @message_key = message_key
      @add_prefix = add_prefix
      @add_suffix = add_suffix

      @consumer = Poseidon::ConsumerGroup.new(
        consumer_group,
        broker_list,
        zookeeper_list,
        topic,
        options
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

      @consumer.fetch do |partition, bulk|
        bulk.each do |msg|
          begin
            msg_record = parse_line(msg.value)
            es.add(Time.now.to_i, msg_record)
          rescue
            $log.warn msg_record.to_s, :error=>$!.to_s
            $log.debug_backtrace
          end
        end
      end

      unless es.empty?
        Engine.emit_stream(tag, es)
      end
    end

    def parse_line(record)
      parsed_record = {}
      case @format
      when 'json'
        parsed_record = Yajl::Parser.parse(record)
      when 'ltsv'
        parsed_record = LTSV.parse(record)
      when 'msgpack'
        parsed_record = MessagePack.unpack(record)
      when 'text'
        parsed_record[@message_key] = record
      end
      parsed_record
    end
  end
end

end
