require 'fluent/input'
require 'fluent/time'
require 'fluent/plugin/kafka_plugin_util'

require 'rdkafka'

class Fluent::RdKafkaGroupInput < Fluent::Input
  Fluent::Plugin.register_input('rdkafka_group', self)

  config_param :topics, :string,
               :desc => "Listening topics(separate with comma',')."

  config_param :format, :string, :default => 'json',
               :desc => "Supported format: (json|text|ltsv|msgpack)"
  config_param :message_key, :string, :default => 'message',
               :desc => "For 'text' format only."
  config_param :add_headers, :bool, :default => false,
               :desc => "Add kafka's message headers to event record"
  config_param :add_prefix, :string, :default => nil,
               :desc => "Tag prefix (Optional)"
  config_param :add_suffix, :string, :default => nil,
               :desc => "Tag suffix (Optional)"
  config_param :use_record_time, :bool, :default => false,
               :desc => "Replace message timestamp with contents of 'time' field.",
               :deprecated => "Use 'time_source record' instead."
  config_param :time_source, :enum, :list => [:now, :kafka, :record], :default => :now,
               :desc => "Source for message timestamp."
  config_param :record_time_key, :string, :default => 'time',
               :desc => "Time field when time_source is 'record'"
  config_param :time_format, :string, :default => nil,
               :desc => "Time format to be used to parse 'time' field."
  config_param :kafka_message_key, :string, :default => nil,
               :desc => "Set kafka's message key to this field"

  config_param :retry_emit_limit, :integer, :default => nil,
               :desc => "How long to stop event consuming when BufferQueueLimitError happens. Wait retry_emit_limit x 1s. The default is waiting until BufferQueueLimitError is resolved"
  config_param :retry_wait_seconds, :integer, :default => 30
  config_param :disable_retry_limit, :bool, :default => false,
               :desc => "If set true, it disables retry_limit and make Fluentd retry indefinitely (default: false)"
  config_param :retry_limit, :integer, :default => 10,
               :desc => "The maximum number of retries for connecting kafka (default: 10)"
 
  config_param :max_wait_time_ms, :integer, :default => 250,
               :desc => "How long to block polls in milliseconds until the server sends us data."
  config_param :max_batch_size, :integer, :default => 10000,
               :desc => "Maximum number of log lines emitted in a single batch."
 
  config_param :kafka_configs, :hash, :default => {},
               :desc => "Kafka configuration properties as desribed in https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md"

  include Fluent::KafkaPluginUtil::SSLSettings
  include Fluent::KafkaPluginUtil::SaslSettings

  class ForShutdown < StandardError
  end

  BufferError = if defined?(Fluent::Plugin::Buffer::BufferOverflowError)
                  Fluent::Plugin::Buffer::BufferOverflowError
                else
                  Fluent::BufferQueueLimitError
                end

  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def initialize
    super

    @time_parser = nil
    @retry_count = 1
  end

  def _config_to_array(config)
    config_array = config.split(',').map {|k| k.strip }
    if config_array.empty?
      raise Fluent::ConfigError, "kafka_group: '#{config}' is a required parameter"
    end
    config_array
  end

  def multi_workers_ready?
    true
  end

  private :_config_to_array

  def configure(conf)
    super

    $log.info "Will watch for topics #{@topics} at brokers " \
              "#{@brokers} and '#{@consumer_group}' group"

    @topics = _config_to_array(@topics)

    @parser_proc = setup_parser

    @time_source = :record if @use_record_time

    if @time_source == :record and @time_format
      if defined?(Fluent::TimeParser)
        @time_parser = Fluent::TimeParser.new(@time_format)
      else
        @time_parser = Fluent::TextParser::TimeParser.new(@time_format)
      end
    end
  end

  def setup_parser
    case @format
    when 'json'
      begin
        require 'oj'
        Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
        Proc.new { |msg| Oj.load(msg.payload) }
      rescue LoadError
        require 'yajl'
        Proc.new { |msg| Yajl::Parser.parse(msg.payload) }
      end
    when 'ltsv'
      require 'ltsv'
      Proc.new { |msg| LTSV.parse(msg.payload, {:symbolize_keys => false}).first }
    when 'msgpack'
      require 'msgpack'
      Proc.new { |msg| MessagePack.unpack(msg.payload) }
    when 'text'
      Proc.new { |msg| {@message_key => msg.payload} }
    end
  end

  def start
    super

    @consumer = setup_consumer
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    # This nil assignment should be guarded by mutex in multithread programming manner.
    # But the situation is very low contention, so we don't use mutex for now.
    # If the problem happens, we will add a guard for consumer.
    consumer = @consumer
    @consumer = nil
    consumer.close

    @thread.join
    super
  end

  def setup_consumer
    consumer = Rdkafka::Config.new(@kafka_configs).consumer
    consumer.subscribe(*@topics)
    consumer
  end

  def reconnect_consumer
    log.warn "Stopping Consumer"
    consumer = @consumer
    @consumer = nil
    if consumer
      consumer.close
    end
    log.warn "Could not connect to broker. retry_time:#{@retry_count}. Next retry will be in #{@retry_wait_seconds} seconds"
    @retry_count = @retry_count + 1
    sleep @retry_wait_seconds
    @consumer = setup_consumer
    log.warn "Re-starting consumer #{Time.now.to_s}"
    @retry_count = 0
  rescue =>e
    log.error "unexpected error during re-starting consumer object access", :error => e.to_s
    log.error_backtrace
    if @retry_count <= @retry_limit or disable_retry_limit
      reconnect_consumer
    end
  end

  class Batch
    attr_reader :topic
    attr_reader :messages

    def initialize(topic)
      @topic = topic
      @messages = []
    end
  end

  def each_batch(&block)
    batch = nil
    message = nil
    while @consumer
      message = @consumer.poll(@max_wait_time_ms)
      if message
        if not batch
          batch = Batch.new(message.topic)
        elsif batch.topic != message.topic || batch.messages.count >= @max_batch_size
          yield batch
          batch = Batch.new(message.topic)
        end
        batch.messages << message
      else
        yield batch if batch
        batch = nil
      end
    end
    yield batch if batch
  end

  def run
    while @consumer
      begin
        each_batch { |batch|
          log.debug "A new batch for topic #{batch.topic} with #{batch.messages.count} messages"
          es = Fluent::MultiEventStream.new
          tag = batch.topic
          tag = @add_prefix + "." + tag if @add_prefix
          tag = tag + "." + @add_suffix if @add_suffix

          batch.messages.each { |msg|
            begin
              record = @parser_proc.call(msg)
              case @time_source
              when :kafka
                record_time = Fluent::EventTime.from_time(msg.timestamp)
              when :now
                record_time = Fluent::Engine.now
              when :record
                if @time_format
                  record_time = @time_parser.parse(record[@record_time_key].to_s)
                else
                  record_time = record[@record_time_key]
                end
              else
                log.fatal "BUG: invalid time_source: #{@time_source}"
              end
              if @kafka_message_key
                record[@kafka_message_key] = msg.key
              end
              if @add_headers
                msg.headers.each_pair { |k, v|
                  record[k] = v
                }
              end
              es.add(record_time, record)
            rescue => e
              log.warn "parser error in #{msg.topic}/#{msg.partition}", :error => e.to_s, :value => msg.payload, :offset => msg.offset
              log.debug_backtrace
            end
          }

          unless es.empty?
            emit_events(tag, es)
          end
        }
      rescue ForShutdown
      rescue => e
        log.error "unexpected error during consuming events from kafka. Re-fetch events.", :error => e.to_s
        log.error_backtrace
        reconnect_consumer
      end
    end
  rescue => e
    log.error "unexpected error during consumer object access", :error => e.to_s
    log.error_backtrace
  end

  def emit_events(tag, es)
    retries = 0
    begin
      router.emit_stream(tag, es)
    rescue BufferError
      raise ForShutdown if @consumer.nil?

      if @retry_emit_limit.nil?
        sleep 1
        retry
      end

      if retries < @retry_emit_limit
        retries += 1
        sleep 1
        retry
      else
        raise RuntimeError, "Exceeds retry_emit_limit"
      end
    end
  end
end
