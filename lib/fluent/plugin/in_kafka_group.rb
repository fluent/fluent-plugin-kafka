require 'fluent/input'
require 'fluent/plugin/kafka_plugin_util'

module Fluent

class KafkaGroupInput < Input
  Plugin.register_input('kafka_group', self)

  config_param :brokers, :string, :default => 'localhost:9092',
               :desc => "List of broker-host:port, separate with comma, must set."
  config_param :consumer_group, :string,
               :desc => "Consumer group name, must set."
  config_param :topics, :string,
               :desc => "Listening topics(separate with comma',')."
  config_param :format, :string, :default => 'json',
               :desc => "Supported format: (json|text|ltsv|msgpack)"
  config_param :message_key, :string, :default => 'message',
               :desc => "For 'text' format only."
  config_param :add_prefix, :string, :default => nil,
               :desc => "Tag prefix (Optional)"
  config_param :add_suffix, :string, :default => nil,
               :desc => "Tag suffix (Optional)"

  # Kafka consumer options
  config_param :max_wait_time, :integer, :default => nil,
               :desc => "How long to block until the server sends us data."
  config_param :min_bytes, :integer, :default => nil,
               :desc => "Smallest amount of data the server should send us."
  config_param :session_timeout, :integer, :default => nil,
               :desc => "The number of seconds after which, if a client hasn't contacted the Kafka cluster"
  config_param :offset_commit_interval, :integer, :default => nil,
               :desc => "The interval between offset commits, in seconds"
  config_param :offset_commit_threshold, :integer, :default => nil,
               :desc => "The number of messages that can be processed before their offsets are committed"
  config_param :start_from_beginning, :bool, :default => true,
               :desc => "Whether to start from the beginning of the topic or just subscribe to new messages being produced"

  include KafkaPluginUtil::SSLSettings

  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def initialize
    super
    require 'kafka'
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

    $log.info "Will watch for topics #{@topics} at brokers " \
              "#{@brokers} and '#{@consumer_group}' group"

    @topics = _config_to_array(@topics)

    if conf['max_wait_ms']
      log.warn "'max_wait_ms' parameter is deprecated. Use second unit 'max_wait_time' instead"
      @max_wait_time = conf['max_wait_ms'].to_i / 1000
    end

    @parser_proc = setup_parser
  end

  def setup_parser
    case @format
    when 'json'
      require 'yajl'
      Proc.new { |msg| Yajl::Parser.parse(msg.value) }
    when 'ltsv'
      require 'ltsv'
      Proc.new { |msg| LTSV.parse(msg.value).first }
    when 'msgpack'
      require 'msgpack'
      Proc.new { |msg| MessagePack.unpack(msg.value) }
    when 'text'
      Proc.new { |msg| {@message_key => msg.value} }
    end
  end

  def start
    super

    consumer_opts = {:group_id => @consumer_group}
    consumer_opts[:session_timeout] = @session_timeout if @session_timeout
    consumer_opts[:offset_commit_interval] = @offset_commit_interval if @offset_commit_interval
    consumer_opts[:offset_commit_threshold] = @offset_commit_threshold if @offset_commit_threshold

    @fetch_opts = {}
    @fetch_opts[:max_wait_time] = @max_wait_time if @max_wait_time
    @fetch_opts[:min_bytes] = @min_bytes if @min_bytes

    @kafka = Kafka.new(seed_brokers: @brokers,
                       ssl_ca_cert: read_ssl_file(@ssl_ca_cert),
                       ssl_client_cert: read_ssl_file(@ssl_client_cert),
                       ssl_client_cert_key: read_ssl_file(@ssl_client_cert_key))
    @consumer = @kafka.consumer(consumer_opts)
    @topics.each { |topic|
      @consumer.subscribe(topic, start_from_beginning: @start_from_beginning)
    }
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @consumer.stop
    @thread.join
    @kafka.close
    super
  end

  def run
    @consumer.each_batch(@fetch_opts) { |batch|
      es = MultiEventStream.new
      tag = batch.topic
      tag = @add_prefix + "." + tag if @add_prefix
      tag = tag + "." + @add_suffix if @add_suffix

      batch.messages.each { |msg|
        begin
          es.add(Engine.now, @parser_proc.call(msg))
        rescue => e
          $log.warn "parser error in #{batch.topic}/#{batch.partition}", :error => e.to_s, :value => msg.value, :offset => msg.offset
          $log.debug_backtrace
        end
      }

      unless es.empty?
        router.emit_stream(tag, es)
      end
    }
  rescue => e
    $log.error "unexpected error", :error => e.to_s
    $log.error_backtrace
  end
end

end
