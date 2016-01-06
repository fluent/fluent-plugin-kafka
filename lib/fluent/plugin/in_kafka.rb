module Fluent

class KafkaInput < Input
  Plugin.register_input('kafka', self)

  config_param :format, :string, :default => 'json',
               :desc => "Supported format: (json|text|ltsv|msgpack)"
  config_param :message_key, :string, :default => 'message',
               :desc => "For 'text' format only."
  config_param :host, :string, :default => 'localhost',
               :desc => "Broker host"
  config_param :port, :integer, :default => 9092,
               :desc => "Broker port"
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

  # poseidon PartitionConsumer options
  config_param :max_bytes, :integer, :default => nil,
               :desc => "Maximum number of bytes to fetch."
  config_param :max_wait_ms, :integer, :default => nil,
               :desc => "How long to block until the server sends us data."
  config_param :min_bytes, :integer, :default => nil,
               :desc => "Smallest amount of data the server should send us."
  config_param :socket_timeout_ms, :integer, :default => nil,
               :desc => "How long to wait for reply from server. Should be higher than max_wait_ms."

  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def initialize
    super
    require 'poseidon'
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

    @zookeeper = Zookeeper.new(@offset_zookeeper) if @offset_zookeeper

    @topic_watchers = @topic_list.map {|topic_entry|
      offset_manager = OffsetManager.new(topic_entry, @zookeeper, @offset_zk_root_node) if @offset_zookeeper
      TopicWatcher.new(
        topic_entry,
        @host,
        @port,
        @client_id,
        interval,
        @format,
        @message_key,
        @add_offset_in_record,
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
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  class TopicWatcher < Coolio::TimerWatcher
    def initialize(topic_entry, host, port, client_id, interval, format, message_key, add_offset_in_record, add_prefix, add_suffix, offset_manager, router, options={})
      @topic_entry = topic_entry
      @host = host
      @port = port
      @client_id = client_id
      @callback = method(:consume)
      @format = format
      @message_key = message_key
      @add_offset_in_record = add_offset_in_record
      @add_prefix = add_prefix
      @add_suffix = add_suffix
      @options = options
      @offset_manager = offset_manager
      @router = router

      @next_offset = @topic_entry.offset
      if @topic_entry.offset == -1 && offset_manager
        @next_offset = offset_manager.next_offset
      end
      @consumer = create_consumer(@next_offset)      

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
      tag = @topic_entry.topic
      tag = @add_prefix + "." + tag if @add_prefix
      tag = tag + "." + @add_suffix if @add_suffix

      if @offset_manager && @consumer.next_offset != @next_offset
        @consumer = create_consumer(@next_offset)
      end

      @consumer.fetch.each { |msg|
        begin
          msg_record = parse_line(msg.value)
          msg_record = decorate_offset(msg_record, msg.offset) if @add_offset_in_record
          es.add(Engine.now, msg_record)
        rescue
          $log.warn msg_record.to_s, :error=>$!.to_s
          $log.debug_backtrace
        end
      }

      unless es.empty?
        @router.emit_stream(tag, es)

        if @offset_manager
          next_offset = @consumer.next_offset
          @offset_manager.save_offset(next_offset)
          @next_offset = next_offset
        end
      end
    end

    def create_consumer(offset)
      @consumer.close if @consumer
      Poseidon::PartitionConsumer.new(
        @client_id,             # client_id
        @host,                  # host
        @port,                  # port
        @topic_entry.topic,     # topic
        @topic_entry.partition, # partition
        offset,                 # offset
        @options                # options
      )
    end

    def parse_line(record)
      case @format
      when 'json'
        Yajl::Parser.parse(record)
      when 'ltsv'
        LTSV.parse(record)
      when 'msgpack'
        MessagePack.unpack(record)
      when 'text'
        {@message_key => record}
      end
    end

    def decorate_offset(record, offset)
      case @format
      when 'json'
        add_offset_in_hash(record, @topic_entry.topic, @topic_entry.partition, offset)
      when 'ltsv'
        record.each { |line|
          add_offset_in_hash(line, @topic_entry.topic, @topic_entry.partition, offset)
        }
      when 'msgpack'
        add_offset_in_hash(record, @topic_entry.topic, @topic_entry.partition, offset)
      when 'text'
        add_offset_in_hash(record, @topic_entry.topic, @topic_entry.partition, offset)
      end
      record
    end

    def add_offset_in_hash(hash, topic, partition, offset)
      hash['kafka_topic'] = topic
      hash['kafka_partition'] = partition
      hash['kafka_offset'] = offset
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
