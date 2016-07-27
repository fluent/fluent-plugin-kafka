require 'fluent/input'
module Fluent

class KafkaInput < Input
  Plugin.register_input('kafka', self)

  config_param :format, :string, :default => 'json',
               :desc => "Supported format: (json|text|ltsv|msgpack)"
  config_param :message_key, :string, :default => 'message',
               :desc => "For 'text' format only."
  config_param :host, :string, :default => nil,
               :desc => "Broker host"
  config_param :port, :integer, :default => nil,
               :desc => "Broker port"
  config_param :brokers, :string, :default => 'localhost:9092',
               :desc => "List of broker-host:port, separate with comma, must set."
  config_param :interval, :integer, :default => 1, # seconds
               :desc => "Interval (Unit: seconds)"
  config_param :topics, :string, :default => nil,
               :desc => "Listening topics(separate with comma',')"
  config_param :client_id, :string, :default => 'kafka'
  config_param :partition, :integer, :default => 0,
               :desc => "Listening partition"
  config_param :offset, :integer, :default => -1,
               :desc => "Listening start offset"
  config_param :add_prefix, :string, :default => nil,
               :desc => "Tag prefix"
  config_param :add_suffix, :string, :default => nil,
               :desc => "tag suffix"
  config_param :add_offset_in_record, :bool, :default => false

  config_param :offset_zookeeper, :string, :default => nil
  config_param :offset_zk_root_node, :string, :default => '/fluent-plugin-kafka'

  # Kafka#fetch_messages options
  config_param :max_bytes, :integer, :default => nil,
               :desc => "Maximum number of bytes to fetch."
  config_param :max_wait_time, :integer, :default => nil,
               :desc => "How long to block until the server sends us data."
  config_param :max_wait_ms, :integer, :default => nil,
               :desc => "Deprecated. How long to block until the server sends us data."
  config_param :min_bytes, :integer, :default => nil,
               :desc => "Smallest amount of data the server should send us."
  config_param :socket_timeout_ms, :integer, :default => nil,
               :desc => "How long to wait for reply from server. Should be higher than max_wait_ms."

  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def initialize
    super
    require 'kafka'
    require 'zookeeper'
  end

  def configure(conf)
    super

    @topic_list = []
    if @topics
      @topic_list = @topics.split(',').map { |topic|
        TopicEntry.new(topic.strip, @partition, @offset)
      }
    else
      conf.elements.select { |element| element.name == 'topic' }.each do |element|
        unless element.has_key?('topic')
          raise ConfigError, "kafka: 'topic' is a require parameter in 'topic element'."
        end
        partition = element.has_key?('partition') ? element['partition'].to_i : 0
        offset = element.has_key?('offset') ? element['offset'].to_i : -1
        @topic_list.push(TopicEntry.new(element['topic'], partition, offset))
      end
    end

    if @topic_list.empty?
      raise ConfigError, "kafka: 'topics' or 'topic element' is a require parameter"
    end

    # For backward compatibility
    @brokers = case
               when @host && @port
                 ["#{@host}:#{@port}"]
               when @host
                 ["#{@host}:9092"]
               when @port
                 ["localhost:#{@port}"]
               else
                 @brokers
               end

    if conf['max_wait_ms']
      log.warn "'max_wait_ms' parameter is deprecated. Use second unit 'max_wait_time' instead"
      @max_wait_time = conf['max_wait_ms'].to_i / 1000
    end

    @max_wait_time = @interval if @max_wait_time.nil?

    require 'zookeeper' if @offset_zookeeper

    @parser_proc = setup_parser
  end

  def setup_parser
    case @format
    when 'json'
      require 'yajl'
      Proc.new { |msg, te|
        r = Yajl::Parser.parse(msg.value)
        add_offset_in_hash(r, te, msg.offset) if @add_offset_in_record
        r
      }
    when 'ltsv'
      require 'ltsv'
      Proc.new { |msg, te|
        r = LTSV.parse(msg.value).first
        add_offset_in_hash(r, te, msg.offset) if @add_offset_in_record
        r
      }
    when 'msgpack'
      require 'msgpack'
      Proc.new { |msg, te|
        r = MessagePack.unpack(msg.value)
        add_offset_in_hash(r, te, msg.offset) if @add_offset_in_record
        r
      }
    when 'text'
      Proc.new { |msg, te|
        r = {@message_key => msg.value}
        add_offset_in_hash(r, te, msg.offset) if @add_offset_in_record
        r
      }
    end
  end

  def add_offset_in_hash(hash, te, offset)
    hash['kafka_topic'.freeze] = te.topic
    hash['kafka_partition'.freeze] = te.partition
    hash['kafka_offset'.freeze] = offset
  end

  def start
    super

    @loop = Coolio::Loop.new
    opt = {}
    opt[:max_bytes] = @max_bytes if @max_bytes
    opt[:max_wait_time] = @max_wait_time if @max_wait_time
    opt[:min_bytes] = @min_bytes if @min_bytes

    @kafka = Kafka.new(seed_brokers: @brokers, client_id: @client_id)
    @zookeeper = Zookeeper.new(@offset_zookeeper) if @offset_zookeeper

    @topic_watchers = @topic_list.map {|topic_entry|
      offset_manager = OffsetManager.new(topic_entry, @zookeeper, @offset_zk_root_node) if @offset_zookeeper
      TopicWatcher.new(
        topic_entry,
        @kafka,
        interval,
        @parser_proc,
        @add_prefix,
        @add_suffix,
        offset_manager,
        router,
        opt)
    }
    @topic_watchers.each {|tw|
      tw.attach(@loop)
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
    @zookeeper.close! if @zookeeper
    super
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  class TopicWatcher < Coolio::TimerWatcher
    def initialize(topic_entry, kafka, interval, parser, add_prefix, add_suffix, offset_manager, router, options={})
      @topic_entry = topic_entry
      @kafka = kafka
      @callback = method(:consume)
      @parser = parser
      @add_prefix = add_prefix
      @add_suffix = add_suffix
      @options = options
      @offset_manager = offset_manager
      @router = router

      @next_offset = @topic_entry.offset
      if @topic_entry.offset == -1 && offset_manager
        @next_offset = offset_manager.next_offset
      end
      @fetch_args = {
        topic: @topic_entry.topic,
        partition: @topic_entry.partition,
      }.merge(@options)

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
      offset = @next_offset
      @fetch_args[:offset] = offset
      messages = @kafka.fetch_messages(@fetch_args)

      return if messages.size.zero?

      es = MultiEventStream.new
      tag = @topic_entry.topic
      tag = @add_prefix + "." + tag if @add_prefix
      tag = tag + "." + @add_suffix if @add_suffix

      messages.each { |msg|
        begin
          es.add(Engine.now, @parser.call(msg, @topic_entry))
        rescue => e
          $log.warn "parser error in #{@topic_entry.topic}/#{@topic_entry.partition}", :error => e.to_s, :value => msg.value, :offset => msg.offset
          $log.debug_backtrace
        end
      }
      offset = messages.last.offset + 1

      unless es.empty?
        @router.emit_stream(tag, es)

        if @offset_manager
          @offset_manager.save_offset(offset)
        end
        @next_offset = offset
      end
    end
  end

  class TopicEntry
    def initialize(topic, partition, offset)
      @topic = topic
      @partition = partition
      @offset = offset
    end
    attr_reader :topic, :partition, :offset
  end

  class OffsetManager
    def initialize(topic_entry, zookeeper, zk_root_node)
      @zookeeper = zookeeper
      @zk_path = "#{zk_root_node}/#{topic_entry.topic}/#{topic_entry.partition}/next_offset"
      create_node(@zk_path, topic_entry.topic, topic_entry.partition)
    end

    def create_node(zk_path, topic, partition)
      path = ""
      zk_path.split(/(\/[^\/]+)/).reject(&:empty?).each { |dir|
        path = path + dir
        @zookeeper.create(:path => "#{path}")
      }
      $log.trace "use zk offset node : #{path}"
    end

    def next_offset
      @zookeeper.get(:path => @zk_path)[:data].to_i
    end

    def save_offset(offset)
      @zookeeper.set(:path => @zk_path, :data => offset.to_s)
      $log.trace "update zk offset node : #{offset.to_s}"
    end
  end
end

end
