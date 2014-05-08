class Fluent::KafkaOutputTimeSliced < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('kafka_timesliced', self)

  def initialize
    super
    require 'poseidon'
    @localtime = false
  end

  config_param :brokers, :string, :default => 'localhost:9092'
  config_param :default_topic, :string, :default => nil
  config_param :default_partition, :integer, :default => 0
  config_param :client_id, :string, :default => 'kafka'
  config_param :output_data_type, :string, :default => 'json'
  config_param :tag_key, :string, :default => nil
  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil
  attr_accessor :output_data_type
  attr_accessor :field_separator

  def configure(conf)
    super
    @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
    @producers = {} # keyed by topic:partition
    case @output_data_type
    when 'json'
      require 'json'
    when 'ltsv'
      require 'ltsv'
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
                         elsif @output_data_type =~ /^attr:(.*)$/
                           $1.split(',').map(&:strip).reject(&:empty?)
                         else
                           nil
                         end

    if @time_key
      if @time_format
        tf = Fluent::TimeFormatter.new(@time_format, @localtime)
        @time_format_proc = tf.method(:format)
      else
        @time_format_proc = Proc.new { |time| time.to_s }
      end
    end
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    if @time_key
      record[@time_key] = @time_format_proc.call(time)
    end
    if @tag_key
      record[@tag_key] = tag
    end
    [tag, time, record].to_msgpack
  end

  def parse_record(record)
    if @custom_attributes.nil?
      case @output_data_type
      when 'json'
        JSON.dump(record)
      when 'ltsv'
        LTSV.dump(record)
      else
        record.to_s
      end
    else
      @custom_attributes.map { |attr|
        record[attr].nil? ? '' : record[attr].to_s
      }.join(@f_separator)
    end
  end

  def write(chunk)
    records_by_topic = {}
    chunk.msgpack_each { |tag, time, record|
      topic = record['topic'] || self.default_topic || tag
      partition = record['partition'] || self.default_partition
      message = Poseidon::MessageToSend.new(topic, parse_record(record))
      records_by_topic[topic] ||= []
      records_by_topic[topic][partition] ||= []
      records_by_topic[topic][partition] << message
    }
    publish(records_by_topic)
  end

  def publish(records_by_topic)
    records_by_topic.each { |topic, partitions|
      partitions.each_with_index { |messages, partition|
        next if not messages
        @producers[topic] ||= Poseidon::Producer.new(@seed_brokers, self.client_id)
        @producers[topic].send_messages(messages)
      }
    }
  end
end
