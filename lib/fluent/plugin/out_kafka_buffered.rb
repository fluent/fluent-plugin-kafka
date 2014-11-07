# encode: utf-8
class Fluent::KafkaOutputBuffered < Fluent::BufferedOutput
  Fluent::Plugin.register_output('kafka_buffered', self)

  def initialize
    super
    require 'poseidon'
  end

  config_param :brokers, :string, :default => 'localhost:9092'
  config_param :default_topic, :string, :default => nil
  config_param :default_partition, :integer, :default => 0
  config_param :client_id, :string, :default => 'kafka'
  config_param :output_data_type, :string, :default => 'json'
  config_param :output_include_tag, :bool, :default => false
  config_param :output_include_time, :bool, :default => false
  config_param :kafka_agg_max_bytes, :size, :default => 4*1024  #4k

  # poseidon producer options
  config_param :max_send_retries, :integer, :default => 3
  config_param :required_acks, :integer, :default => 0
  config_param :ack_timeout_ms, :integer, :default => 1500

  attr_accessor :output_data_type
  attr_accessor :field_separator

  unless method_defined?(:log)
    define_method("log") { $log }
  end

  def configure(conf)
    super
    @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
    case @output_data_type
    when 'json'
      require 'yajl'
    when 'ltsv'
      require 'ltsv'
    when 'msgpack'
      require 'msgpack'
    end

    @f_separator = case @field_separator
                   when /SPACE/i then ' '
                   when /COMMA/i then ','
                   when /SOH/i then "\x01"
                   else "\t"
                   end

    @custom_attributes = if @output_data_type == 'json'
                           nil
                         elsif @output_data_type == 'ltsv'
                           nil
                         elsif @output_data_type == 'msgpack'
                           nil
                         elsif @output_data_type =~ /^attr:(.*)$/
                           $1.split(',').map(&:strip).reject(&:empty?)
                         else
                           nil
                         end
  end

  def start
    super
    @producer = Poseidon::Producer.new(@seed_brokers, @client_id, :max_send_retries => @max_send_retries, :required_acks => @required_acks, :ack_timeout_ms => @ack_timeout_ms)
    log.info "initialized producer #{@client_id}"
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def parse_record(record)
    if @custom_attributes.nil?
      case @output_data_type
      when 'json'
        Yajl::Encoder.encode(record)
      when 'ltsv'
        LTSV.dump(record)
      when 'msgpack'
        record.to_msgpack
      else
        record.to_s
      end
    else
      @custom_attributes.unshift('time') if @output_include_time
      @custom_attributes.unshift('tag') if @output_include_tag
      @custom_attributes.map { |attr|
        record[attr].nil? ? '' : record[attr].to_s
      }.join(@f_separator)
    end
  end

  def write(chunk)
    records_by_topic = {}
    bytes_by_topic = {}
    messages = []
    messages_bytes = 0
    chunk.msgpack_each { |tag, time, record|
      record['time'] = time if @output_include_time
      record['tag'] = tag if @output_include_tag
      topic = record['topic'] || @default_topic || tag

      records_by_topic[topic] ||= 0
      bytes_by_topic[topic] ||= 0

      record_buf = parse_record(record)
      record_buf_bytes = record_buf.bytesize
      if messages.length > 0 and messages_bytes + record_buf_bytes > @kafka_agg_max_bytes
        @producer.send_messages(messages)
        messages = []
        messages_bytes = 0
      end
      messages << Poseidon::MessageToSend.new(topic, record_buf)
      messages_bytes += record_buf_bytes

      records_by_topic[topic] += 1
      bytes_by_topic[topic] += record_buf_bytes
    }
    if messages.length > 0
      @producer.send_messages(messages)
    end
    log.debug "(records|bytes) (#{records_by_topic}|#{bytes_by_topic})"
  end

end
