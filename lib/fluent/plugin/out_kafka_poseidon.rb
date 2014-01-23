class Fluent::KafkaPoseidonOutput < Fluent::Output
  Fluent::Plugin.register_output('kafka-poseidon', self)

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

  def emit(tag, es, chain)
    chain.next
    es.each do |time,record|
      topic = record['topic'] || self.default_topic || tag
      partition = record['partition'] || self.default_partition
      message = Poseidon::MessageToSend.new(topic, record.to_s)
      @producers[topic] ||= Poseidon::Producer.new(@seed_brokers, self.client_id)
      @producers[topic].send_messages([message])
    end
  end

end
