# encode: utf-8

class Fluent::KafkaOutputBuffered < Fluent::BufferedOutput
  Fluent::Plugin.register_output('kafka_buffered', self)

  def initialize
    super

    require 'kafka'

    @kafka = nil
    @producer = nil
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
  config_param :kafka_agg_max_bytes, :size, :default => 4*1024*1024  #4k

  # poseidon producer options
  config_param :max_send_retries, :integer, :default => 1,
               :desc => "Number of times to retry sending of messages to a leader."
  config_param :required_acks, :integer, :default => 0,
               :desc => "The number of acks required per request."
  config_param :ack_timeout, :integer, :default => nil,
               :desc => "How long the producer waits for acks."
  config_param :compression_codec, :string, :default => nil,
               :desc => <<-DESC
The codec the producer uses to compress messages.
Supported codecs: (gzip|snappy)
DESC

  config_param :time_format, :string, :default => nil

  attr_accessor :output_data_type
  attr_accessor :field_separator

  unless method_defined?(:log)
    define_method("log") { $log }
  end

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
        @kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id)
        producer_opts = {max_retries: @max_send_retries, required_acks: @required_acks,
                         max_buffer_size: @buffer.buffer_chunk_limit / 10, max_buffer_bytesize: @buffer.buffer_chunk_limit * 2}
        producer_opts[:ack_timeout] = @ack_timeout if @ack_timeout
        producer_opts[:compression_codec] = @compression_codec.to_sym if @compression_codec
        @producer = @kafka.producer(producer_opts)
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
    else
      @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
      log.info "brokers has been set directly: #{@seed_brokers}"
    end

    if conf['ack_timeout_ms']
      log.warn "'ack_timeout_ms' parameter is removed. Use second unit 'ack_timeout' instead"
      @ack_timeout = conf['ack_timeout_ms'].to_i / 1000
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
    shutdown_producer
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def shutdown_producer
    if @producer
      @producer.shutdown
      @producer = nil
    end
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
    messages = 0
    messages_bytes = 0
    begin
      chunk.msgpack_each { |tag, time, record|
        if @output_include_time
          if @time_format
            record['time'] = Time.at(time).strftime(@time_format)
          else
            record['time'] = time
          end
        end

        record['tag'] = tag if @output_include_tag
        topic = record['topic'] || @default_topic || tag
        partition_key = record['partition_key'] || @default_partition_key

        records_by_topic[topic] ||= 0
        bytes_by_topic[topic] ||= 0

        record_buf = @formatter_proc.call(tag, time, record)
        record_buf_bytes = record_buf.bytesize
        if messages > 0 and messages_bytes + record_buf_bytes > @kafka_agg_max_bytes
          log.on_trace { log.trace("#{messages} messages send.") }
          @producer.deliver_messages
          messages = 0
          messages_bytes = 0
        end
        log.on_trace { log.trace("message will send to #{topic} with key: #{partition_key} and value: #{record_buf}.") }
        messages += 1
        @producer.produce(record_buf, topic: topic, partition_key: partition_key)
        messages_bytes += record_buf_bytes

        records_by_topic[topic] += 1
        bytes_by_topic[topic] += record_buf_bytes
      }
      if messages > 0
        log.trace("#{messages} messages send.")
        @producer.deliver_messages
      end
      log.debug "(records|bytes) (#{records_by_topic}|#{bytes_by_topic})"
    end
  rescue Exception => e
    log.warn "Send exception occurred: #{e}"
    shutdown_producer
    refresh_producer()
    # Raise exception to retry sendind messages
    raise e
  end
end
