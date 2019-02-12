require 'thread'
require 'fluent/plugin/output'
require 'fluent/plugin/kafka_plugin_util'

require 'rdkafka'

class Rdkafka::Producer
  # return false if producer is forcefully closed, otherwise return true
  def close(timeout = nil)
    @closing = true
    # Wait for the polling thread to finish up
    # If the broker isn't alive, the thread doesn't exit
    if timeout
      thr = @polling_thread.join(timeout)
      return !!thr
    else
      @polling_thread.join
      return true
    end
  end
end

module Fluent::Plugin
  class Fluent::Rdkafka2Output < Output
    Fluent::Plugin.register_output('rdkafka2', self)

    helpers :inject, :formatter

    config_param :brokers, :string, :default => 'localhost:9092',
                 :desc => <<-DESC
Set brokers directly:
<broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
Brokers: you can choose to use either brokers or zookeeper.
DESC
    config_param :topic_key, :string, :default => 'topic', :desc => "Field for kafka topic"
    config_param :default_topic, :string, :default => nil,
                 :desc => "Default output topic when record doesn't have topic field"
    config_param :message_key_key, :string, :default => 'message_key', :desc => "Field for kafka message key"
    config_param :default_message_key, :string, :default => nil
    config_param :partition_key, :string, :default => 'partition', :desc => "Field for kafka partition"
    config_param :default_partition, :integer, :default => nil
    config_param :client_id, :string, :default => 'kafka'
    config_param :output_data_type, :string, :default => 'json', :obsoleted => "Use <format> section instead"
    config_param :output_include_tag, :bool, :default => false, :obsoleted => "Use <inject> section instead"
    config_param :output_include_time, :bool, :default => false, :obsoleted => "Use <inject> section instead"
    config_param :exclude_partition, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove partition from data
DESC
    config_param :exclude_message_key, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove partition key from data
DESC
    config_param :exclude_topic_key, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove topic name key from data
DESC
    config_param :max_send_retries, :integer, :default => 2,
                 :desc => "Number of times to retry sending of messages to a leader."
    config_param :required_acks, :integer, :default => -1,
                 :desc => "The number of acks required per request."
    config_param :ack_timeout, :time, :default => nil,
                 :desc => "How long the producer waits for acks."
    config_param :compression_codec, :string, :default => nil,
                 :desc => <<-DESC
The codec the producer uses to compress messages.
Supported codecs: (gzip|snappy)
DESC

    config_param :rdkafka_buffering_max_ms, :integer, :default => nil
    config_param :rdkafka_buffering_max_messages, :integer, :default => nil
    config_param :rdkafka_message_max_bytes, :integer, :default => nil
    config_param :rdkafka_message_max_num, :integer, :default => nil
    config_param :rdkafka_delivery_handle_poll_timeout, :integer, :default => 30
    config_param :rdkafka_options, :hash, :default => {}

    config_param :max_enqueue_retries, :integer, :default => 3
    config_param :enqueue_retry_backoff, :integer, :default => 3

    config_param :service_name, :string, :default => nil
    config_param :ssl_client_cert_key_password, :string, :default => nil

    config_section :buffer do
      config_set_default :chunk_keys, ["topic"]
    end
    config_section :format do
      config_set_default :@type, 'json'
      config_set_default :add_newline, false
    end

    include Fluent::KafkaPluginUtil::SSLSettings
    include Fluent::KafkaPluginUtil::SaslSettings

    def initialize
      super
      @producers = {}
      @producers_mutex = Mutex.new
    end

    def configure(conf)
      super
      log.instance_eval {
        def add(level, &block)
          if block
            self.info(block.call)
          end
        end
      }
      Rdkafka::Config.logger = log
      config = build_config
      @rdkafka = Rdkafka::Config.new(config)

      if @default_topic.nil?
        if @chunk_keys.include?(@topic_key) && !@chunk_key_tag
          log.warn "Use '#{@topic_key}' field of event record for topic but no fallback. Recommend to set default_topic or set 'tag' in buffer chunk keys like <buffer #{@topic_key},tag>"
        end
      else
        if @chunk_key_tag
          log.warn "default_topic is set. Fluentd's event tag is not used for topic"
        end
      end

      formatter_conf = conf.elements('format').first
      unless formatter_conf
        raise Fluent::ConfigError, "<format> section is required."
      end
      unless formatter_conf["@type"]
        raise Fluent::ConfigError, "format/@type is required."
      end
      @formatter_proc = setup_formatter(formatter_conf)
      @topic_key_sym = @topic_key.to_sym
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

      @rdkafka_options.each { |k, v|
        config[k.to_sym] = v
      }

      config
    end

    def start
      super
    end

    def multi_workers_ready?
      true
    end

    def shutdown
      super
      shutdown_producers
    end

    def shutdown_producers
      @producers_mutex.synchronize {
        shutdown_threads = @producers.map { |key, producer|
          th = Thread.new {
            unless producer.close(10)
              log.warn("Queue is forcefully closed after 10 seconds wait")
            end
          }
          th.abort_on_exception = true
          th
        }
        shutdown_threads.each { |th| th.join }
        @producers = {}
      }
    end

    def get_producer
      @producers_mutex.synchronize {
        producer = @producers[Thread.current.object_id]
        unless producer
          producer = @rdkafka.producer
          @producers[Thread.current.object_id] = producer
        end
        producer
      }
    end

    def setup_formatter(conf)
      type = conf['@type']
      case type
      when 'ltsv'
        require 'ltsv'
        Proc.new { |tag, time, record| LTSV.dump(record) }
      else
        @formatter = formatter_create(usage: 'rdkafka-plugin', conf: conf)
        @formatter.method(:format)
      end
    end

    def write(chunk)
      tag = chunk.metadata.tag
      topic = chunk.metadata.variables[@topic_key_sym] || @default_topic || tag

      handlers = []
      record_buf = nil
      record_buf_bytes = nil

      begin
        producer = get_producer
        chunk.msgpack_each { |time, record|
          begin
            record = inject_values_to_record(tag, time, record)
            record.delete(@topic_key) if @exclude_topic_key
            partition = (@exclude_partition ? record.delete(@partition_key) : record[@partition_key]) || @default_partition
            message_key = (@exclude_message_key ? record.delete(@message_key_key) : record[@message_key_key]) || @default_message_key

            record_buf = @formatter_proc.call(tag, time, record)
            record_buf_bytes = record_buf.bytesize
            if @max_send_limit_bytes && record_buf_bytes > @max_send_limit_bytes
              log.warn "record size exceeds max_send_limit_bytes. Skip event:", :time => time, :record => record
              next
            end
          rescue StandardError => e
            log.warn "unexpected error during format record. Skip broken event:", :error => e.to_s, :error_class => e.class.to_s, :time => time, :record => record
            next
          end

          handlers << enqueue_with_retry(producer, topic, record_buf, message_key, partition)
        }
        handlers.each { |handler|
          handler.wait(@rdkafka_delivery_handle_poll_timeout) if @rdkafka_delivery_handle_poll_timeout != 0
        }
      end
    rescue Exception => e
      log.warn "Send exception occurred: #{e} at #{e.backtrace.first}"
      # Raise exception to retry sendind messages
      raise e
    end

    def enqueue_with_retry(producer, topic, record_buf, message_key, partition)
      attempt = 0
      loop do
        begin
          return producer.produce(topic: topic, payload: record_buf, key: message_key, partition: partition)
        rescue Exception => e
          if e.code == :queue_full
            if attempt <= @max_enqueue_retries
              log.warn "Failed to enqueue message; attempting retry #{attempt} of #{@max_enqueue_retries} after #{@enqueue_retry_backoff}s"
              sleep @enqueue_retry_backoff
              attempt += 1
            else
              raise "Failed to enqueue message although tried retry #{@max_enqueue_retries} times"
            end
          else
            raise e
          end
        end
      end
    end
  end
end
