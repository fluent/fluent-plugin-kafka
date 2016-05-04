# encode: utf-8
class Fluent::KafkaOutputBuffered < Fluent::BufferedOutput
  Fluent::Plugin.register_output('kafka_buffered', self)

  def initialize
    super
    require 'poseidon'
  end

  config_param :brokers, :string, :default => 'localhost:9092',
               :desc => <<-DESC
Set brokers directly:
<broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
Brokers: you can choose to use either brokers or zookeeper.
DESC
  config_param :zookeeper, :string, :default => nil,
               :desc => <<-DESC
Set brokers via Zookeeper:
<zookeeper_host>:<zookeeper_port>
DESC
  config_param :zookeeper_path, :string, :default => '/brokers/ids',
               :desc => "Path in path for Broker id. Default to /brokers/ids"
  config_param :default_topic, :string, :default => nil,
               :desc => "Output topic"
  config_param :default_partition_key, :string, :default => nil
  config_param :client_id, :string, :default => 'kafka'
  config_param :output_data_type, :string, :default => 'json',
               :desc => <<-DESC
Supported format: (json|ltsv|msgpack|attr:<record name>|<formatter name>)
DESC
  config_param :output_include_tag, :bool, :default => false
  config_param :output_include_time, :bool, :default => false
  config_param :kafka_agg_max_bytes, :size, :default => 4*1024  #4k

  # poseidon producer options
  config_param :max_send_retries, :integer, :default => 3,
               :desc => "Number of times to retry sending of messages to a leader."
  config_param :required_acks, :integer, :default => 0,
               :desc => "The number of acks required per request."
  config_param :ack_timeout_ms, :integer, :default => 1500,
               :desc => "How long the producer waits for acks."
  config_param :compression_codec, :string, :default => 'none',
               :desc => <<-DESC
The codec the producer uses to compress messages.
Supported codecs: (none|gzip|snappy)
DESC

  attr_accessor :output_data_type
  attr_accessor :field_separator

  unless method_defined?(:log)
    define_method("log") { $log }
  end

  @seed_brokers = []

  def refresh_producer()
    if @zookeeper
      @seed_brokers = []
      z = Zookeeper.new(@zookeeper)
      z.get_children(:path => @zookeeper_path)[:children].each do |id|
        broker = Yajl.load(z.get(:path => @zookeeper_path + "/#{id}")[:data])
        @seed_brokers.push("#{broker['host']}:#{broker['port']}")
      end
      z.close
      log.info "brokers has been refreshed via Zookeeper: #{@seed_brokers}"
    end
    begin
      if @seed_brokers.length > 0
        @producer = Poseidon::Producer.new(@seed_brokers, @client_id, :max_send_retries => @max_send_retries, :required_acks => @required_acks, :ack_timeout_ms => @ack_timeout_ms, :compression_codec => @compression_codec.to_sym)
        log.info "initialized producer #{@client_id}"
      else
        log.warn "No brokers found on Zookeeper"
      end
    rescue Exception => e
      log.error e
    end
  end

  def configure(conf)
    super
    if @zookeeper
      require 'zookeeper'
      require 'yajl'
    else
      @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
      log.info "brokers has been set directly: #{@seed_brokers}"
    end
    if @compression_codec == 'snappy'
      require 'snappy'
    end

    @f_separator = case @field_separator
                   when /SPACE/i then ' '
                   when /COMMA/i then ','
                   when /SOH/i then "\x01"
                   else "\t"
                   end

    @formatter_proc = setup_formatter(conf)
  end

  def start
    super
    refresh_producer()
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def setup_formatter(conf)
    if @output_data_type == 'json'
      require 'yajl'
      Proc.new { |tag, time, record| Yajl::Encoder.encode(record) }
    elsif @output_data_type == 'ltsv'
      require 'ltsv'
      Proc.new { |tag, time, record| LTSV.dump(record) }
    elsif @output_data_type == 'msgpack'
      require 'msgpack'
      Proc.new { |tag, time, record| record.to_msgpack }
    elsif @output_data_type =~ /^attr:(.*)$/
      @custom_attributes = $1.split(',').map(&:strip).reject(&:empty?)
      @custom_attributes.unshift('time') if @output_include_time
      @custom_attributes.unshift('tag') if @output_include_tag
      Proc.new { |tag, time, record|
        @custom_attributes.map { |attr|
          record[attr].nil? ? '' : record[attr].to_s
        }.join(@f_separator)
      }
    else
      @formatter = Fluent::Plugin.new_formatter(@output_data_type)
      @formatter.configure(conf)
      Proc.new { |tag, time, record|
        @formatter.format(tag, time, record)
      }
    end
  end

  def write(chunk)
    records_by_topic = {}
    bytes_by_topic = {}
    messages = []
    messages_bytes = 0
    begin
      chunk.msgpack_each { |tag, time, record|
        record['time'] = time if @output_include_time
        record['tag'] = tag if @output_include_tag
        topic = record['topic'] || @default_topic || tag
        partition_key = record['partition_key'] || @default_partition_key

        records_by_topic[topic] ||= 0
        bytes_by_topic[topic] ||= 0

        record_buf = @formatter_proc.call(tag, time, record)
        record_buf_bytes = record_buf.bytesize
        if messages.length > 0 and messages_bytes + record_buf_bytes > @kafka_agg_max_bytes
          log.on_trace { log.trace("#{messages.length} messages send.") }
          @producer.send_messages(messages)
          messages = []
          messages_bytes = 0
        end
        log.on_trace { log.trace("message will send to #{topic} with key: #{partition_key} and value: #{record_buf}.") }
        messages << Poseidon::MessageToSend.new(topic, record_buf, partition_key)
        messages_bytes += record_buf_bytes

        records_by_topic[topic] += 1
        bytes_by_topic[topic] += record_buf_bytes
      }
      if messages.length > 0
        log.trace("#{messages.length} messages send.")
        @producer.send_messages(messages)
      end
      log.debug "(records|bytes) (#{records_by_topic}|#{bytes_by_topic})"
    end
  rescue Exception => e
    log.warn "Send exception occurred: #{e}"
    @producer.close if @producer
    refresh_producer()
    # Raise exception to retry sendind messages
    raise e
  end
end
