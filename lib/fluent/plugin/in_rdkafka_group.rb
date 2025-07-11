require 'fluent/plugin/input'
require 'fluent/time'
require 'fluent/plugin/kafka_plugin_util'

require 'rdkafka'

class Fluent::Plugin::RdKafkaGroupInput < Fluent::Plugin::Input
  Fluent::Plugin.register_input('rdkafka_group', self)

  helpers :thread, :parser, :compat_parameters

  config_param :brokers, :string, :default => 'localhost:9092',
                 :desc => <<-DESC
Set brokers directly:
<broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
Brokers: you can choose to use either brokers or zookeeper.
DESC

  config_param :group_id, :string, :default => 'fluentd',
                :desc => "A group id for the consumer."

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

  config_param :idempotent, :bool, :default => false, :desc => 'Enable idempotent producer'

  config_param :max_send_retries, :integer, :default => 2,
               :desc => "Number of times to retry sending of messages to a leader. Used for message.send.max.retries"
  config_param :required_acks, :integer, :default => -1,
               :desc => "The number of acks required per request. Used for request.required.acks"
  config_param :ack_timeout, :time, :default => nil,
               :desc => "How long the producer waits for acks. Used for request.timeout.ms"
  config_param :compression_codec, :string, :default => nil,
               :desc => <<-DESC
The codec the producer uses to compress messages. Used for compression.codec
Supported codecs: (gzip|snappy)
DESC

  config_param :rdkafka_buffering_max_ms, :integer, :default => nil, :desc => 'Used for queue.buffering.max.ms'
  config_param :rdkafka_buffering_max_messages, :integer, :default => nil, :desc => 'Used for queue.buffering.max.messages'
  config_param :rdkafka_message_max_bytes, :integer, :default => nil, :desc => 'Used for message.max.bytes'
  config_param :rdkafka_message_max_num, :integer, :default => nil, :desc => 'Used for batch.num.messages'

  config_param :rdkafka_options, :hash, :default => {},
               :desc => "Set any rdkafka configuration. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md"

  config_section :parse do
    config_set_default :@type, 'json'
  end

  include Fluent::KafkaPluginUtil::SSLSettings
  include Fluent::KafkaPluginUtil::SaslSettings

  class ForShutdown < StandardError
  end

  BufferError = Fluent::Plugin::Buffer::BufferOverflowError

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
    compat_parameters_convert(conf, :parser)

    super

    log.warn "The in_rdkafka_group consumer was not yet tested under heavy production load. Use it at your own risk!"

    log.info "Will watch for topics #{@topics} at brokers " \
              "#{@brokers} and '#{@group_id}' group"

    @topics = _config_to_array(@topics)

    parser_conf = conf.elements('parse').first
    unless parser_conf
      raise Fluent::ConfigError, "<parse> section or format parameter is required."
    end
    unless parser_conf["@type"]
      raise Fluent::ConfigError, "parse/@type is required."
    end
    @parser_proc = setup_parser(parser_conf)

    @time_source = :record if @use_record_time

    if @time_source == :record and @time_format
      @time_parser = Fluent::TimeParser.new(@time_format)
    end
  end

  def setup_parser(parser_conf)
    format = parser_conf["@type"]
    case format
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
    else
      @custom_parser = parser_create(usage: 'in-rdkafka-plugin', conf: parser_conf)
      Proc.new { |msg|
        @custom_parser.parse(msg.payload) {|_time, record|
          record
        }
      }
    end
  end

  def build_config
    config = {:"bootstrap.servers" => @brokers}

    if @ssl_ca_cert && @ssl_ca_cert[0]
      ssl = true
      config[:"ssl.ca.location"] = @ssl_ca_cert[0]
      config[:"ssl.certificate.location"] = @ssl_client_cert if @ssl_client_cert
      config[:"ssl.key.location"] = @ssl_client_cert_key if @ssl_client_cert_key
      config[:"ssl.key.password"] = @ssl_client_cert_key_password if @ssl_client_cert_key_password
    end

    if @principal
      sasl = true
      config[:"sasl.mechanisms"] = "GSSAPI"
      config[:"sasl.kerberos.principal"] = @principal
      config[:"sasl.kerberos.service.name"] = @service_name if @service_name
      config[:"sasl.kerberos.keytab"] = @keytab if @keytab
    end

    if ssl && sasl
      security_protocol = "SASL_SSL"
    elsif ssl && !sasl
      security_protocol = "SSL"
    elsif !ssl && sasl
      security_protocol = "SASL_PLAINTEXT"
    else
      security_protocol = "PLAINTEXT"
    end
    config[:"security.protocol"] = security_protocol

    config[:"compression.codec"] = @compression_codec if @compression_codec
    config[:"message.send.max.retries"] = @max_send_retries if @max_send_retries
    config[:"request.required.acks"] = @required_acks if @required_acks
    config[:"request.timeout.ms"] = @ack_timeout * 1000 if @ack_timeout
    config[:"queue.buffering.max.ms"] = @rdkafka_buffering_max_ms if @rdkafka_buffering_max_ms
    config[:"queue.buffering.max.messages"] = @rdkafka_buffering_max_messages if @rdkafka_buffering_max_messages
    config[:"message.max.bytes"] = @rdkafka_message_max_bytes if @rdkafka_message_max_bytes
    config[:"batch.num.messages"] = @rdkafka_message_max_num if @rdkafka_message_max_num
    config[:"sasl.username"] = @username if @username
    config[:"sasl.password"] = @password if @password
    config[:"enable.idempotence"] = @idempotent if @idempotent

    @rdkafka_options.each { |k, v|
      config[k.to_sym] = v
    }

    config
  end

  def start
    super

    @consumer = setup_consumer

    thread_create(:in_rdkafka_group, &method(:run))
  end

  def shutdown
    # This nil assignment should be guarded by mutex in multithread programming manner.
    # But the situation is very low contention, so we don't use mutex for now.
    # If the problem happens, we will add a guard for consumer.
    consumer = @consumer
    @consumer = nil
    consumer.close

    super
  end

  def setup_consumer
    @config = build_config
    consumer = Rdkafka::Config.new(@config).consumer
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

  # Executes the passed codeblock on a batch of messages.
  # It is guaranteed that every message in a given batch belongs to the same topic, because the tagging logic in :run expects that property.
  # The number of maximum messages in a batch is capped by the :max_batch_size configuration value. It ensures that consuming from a single
  # topic for a long time (e.g. with `auto.offset.reset` set to `earliest`) does not lead to memory exhaustion. Also, calling consumer.poll
  # advances thes consumer offset, so in case the process crashes we might lose at most :max_batch_size messages.
  def each_batch(&block)
    batch = nil
    message = nil
    while @consumer
      message = @consumer.poll(@max_wait_time_ms)
      if message
        if not batch
          batch = Batch.new(message.topic)
        elsif batch.topic != message.topic || batch.messages.size >= @max_batch_size
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
          log.debug "A new batch for topic #{batch.topic} with #{batch.messages.size} messages"
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
