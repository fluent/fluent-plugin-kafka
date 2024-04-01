require 'thread'
require 'logger'
require 'fluent/plugin/output'
require 'fluent/plugin/kafka_plugin_util'
require 'waterdrop'

module Fluent::Plugin
  class Fluent::WaterdropOutput < Output
    Fluent::Plugin.register_output('waterdrop', self)
    helpers :inject, :formatter, :record_accessor

    config_param :bootstrap_servers, :string, default: 'localhost:9092',
                 desc: <<-DESC
Set bootstrap servers directly:
<broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
                 DESC

    config_param :default_topic, :string, default: nil, desc: <<-DESC
Default output topic when record doesn't have topic field
    DESC

    config_param :topic_key, :string, :default => 'topic', :desc => "Field for kafka topic"

    config_section :buffer do
      config_set_default :chunk_keys, ["topic"]
    end

    config_section :format do
      config_set_default :@type, 'json'
      config_set_default :add_newline, false
    end

    def initialize
      super

      config = {
        'bootstrap.servers': @bootstrap_servers
      }

      @producer = WaterDrop::Producer.new do |conf|
        conf.deliver = true
        conf.kafka = config
      end

      @formatter_proc = nil
      @topic_key_sym = @topic_key.to_sym
    end

    def configure(conf)
      super

      formatter_conf = conf.elements('format').first
      unless formatter_conf
        raise Fluent::ConfigError, "<format> section is required."
      end
      unless formatter_conf["@type"]
        raise Fluent::ConfigError, "format/@type is required."
      end

      @formatter_proc = setup_formatter(formatter_conf)
    end

    def setup_formatter(conf)
      @formatter = formatter_create(usage: 'waterdrop-plugin', conf: conf)
      @formatter.method(:format)
    end

    def write(chunk)
      tag = chunk.metadata.tag
      topic = if @topic
                extract_placeholders(@topic, chunk)
              else
                (chunk.metadata.variables && chunk.metadata.variables[@topic_key_sym]) || @default_topic || tag
              end
      begin
        chunk.msgpack_each do |time, record|
          record_buf = @formatter_proc.call(tag, time, record)
          @producer.buffer(topic: topic, payload: record_buf)
        end

        @producer.flush_sync
      end
    end

    def shutdown
      super

      @producer.close
    end
  end
end