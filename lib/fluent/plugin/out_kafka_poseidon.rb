class Fluent::KafkaPoseidonOutput < Fluent::Output
  Fluent::Plugin.register_output('kafka_poseidon', self)

  def initialize
    super
    require 'poseidon'
  end

  config_param :brokers, :string, :default => 'localhost:9092'
  config_param :default_topic, :string, :default => nil
  config_param :default_partition, :integer, :default => 0
  config_param :client_id, :string, :default => 'kafka-poseidon'
  config_param :output_data_type, :string, :default => 'json'
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

  end

  def start
    super
  end

  def shutdown
    super
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

  def emit(tag, es, chain)
    chain.next
    es.each do |time,record|
      topic = record['topic'] || self.default_topic || tag
      partition = record['partition'] || self.default_partition
      message = Poseidon::MessageToSend.new(topic, parse_record(record))
      @producers[topic] ||= Poseidon::Producer.new(@seed_brokers, self.client_id)
      @producers[topic].send_messages([message])
    end
  end

end
