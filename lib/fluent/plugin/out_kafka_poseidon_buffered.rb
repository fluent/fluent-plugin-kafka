class Fluent::KafkaPoseidonOutputBuffered < Fluent::BufferedOutput
  Fluent::Plugin.register_output('kafka-poseidon-buffered', self)

  def initialize
    super
    require 'poseidon'
  end

  config_param :brokers, :string, :default => 'localhost:9092'
  config_param :default_topic, :string, :default => nil
  config_param :default_partition, :integer, :default => 0
  config_param :client_id, :string, :default => 'kafka-poseidon'

  def configure(conf)
    super
    @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
    @producers = {} # keyed by topic:partition
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    records_by_topic = {}
    chunk.msgpack_each { |tag, time, record|
      topic = record['topic'] || self.default_topic || tag
      partition = record['partition'] || self.default_partition
      message = Poseidon::MessageToSend.new(topic, record.to_s)
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
