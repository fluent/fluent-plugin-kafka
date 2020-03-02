require 'thread'
require 'logger'
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

    helpers :inject, :formatter, :record_accessor

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
    config_param :output_data_type, :string, :default => 'json', :obsoleted => "Use <format> section instead"
    config_param :output_include_tag, :bool, :default => false, :obsoleted => "Use <inject> section instead"
    config_param :output_include_time, :bool, :default => false, :obsoleted => "Use <inject> section instead"
    config_param :exclude_partition, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove partition from data
DESC
    config_param :exclude_message_key, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove message_key from data
DESC
    config_param :exclude_topic_key, :bool, :default => false,
                 :desc => <<-DESC
Set true to remove topic key from data
DESC
    config_param :headers, :hash, default: {}, symbolize_keys: true, value_type: :string,
                 :desc => 'Kafka message headers'
    config_param :headers_from_record, :hash, default: {}, symbolize_keys: true, value_type: :string,
                 :desc => 'Kafka message headers where the header value is a jsonpath to a record value'

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
    config_param :max_send_limit_bytes, :size, :default => nil
    config_param :rdkafka_buffering_max_ms, :integer, :default => nil, :desc => 'Used for queue.buffering.max.ms'
    config_param :rdkafka_buffering_max_messages, :integer, :default => nil, :desc => 'Used for queue.buffering.max.messages'
    config_param :rdkafka_message_max_bytes, :integer, :default => nil, :desc => 'Used for message.max.bytes'
    config_param :rdkafka_message_max_num, :integer, :default => nil, :desc => 'Used for batch.num.messages'
    config_param :rdkafka_delivery_handle_poll_timeout, :integer, :default => 30, :desc => 'Timeout for polling message wait'
    config_param :rdkafka_options, :hash, :default => {}, :desc => 'Set any rdkafka configuration. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md'
    config_param :share_producer, :bool, :default => false, :desc => 'share kafka producer between flush threads'

    config_param :max_enqueue_retries, :integer, :default => 3
    config_param :enqueue_retry_backoff, :integer, :default => 3

    config_param :service_name, :string, :default => nil, :desc => 'Used for sasl.kerberos.service.name'
    config_param :ssl_client_cert_key_password, :string, :default => nil, :desc => 'Used for ssl.key.password'

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

      @producers = nil
      @producers_mutex = nil
      @shared_producer = nil
    end

    def configure(conf)
      super
      log.instance_eval {
        def add(level, &block)
          return unless block

          # Follow rdkakfa's log level. See also rdkafka-ruby's bindings.rb: https://github.com/appsignal/rdkafka-ruby/blob/e5c7261e3f2637554a5c12b924be297d7dca1328/lib/rdkafka/bindings.rb#L117
          case level
          when Logger::FATAL
            self.fatal(block.call)
          when Logger::ERROR
            self.error(block.call)
          when Logger::WARN
            self.warn(block.call)
          when Logger::INFO
            self.info(block.call)
          when Logger::DEBUG
            self.debug(block.call)
          else
            self.trace(block.call)
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

      @headers_from_record_accessors = {}
      @headers_from_record.each do |key, value|
        @headers_from_record_accessors[key] = record_accessor_create(value)
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

      @rdkafka_options.each { |k, v|
        config[k.to_sym] = v
      }

      config
    end

    def start
      if @share_producer
        @shared_producer = @rdkafka.producer
      else
        @producers = {}
        @producers_mutex = Mutex.new
      end

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
      if @share_producer
        close_producer(@shared_producer)
        @shared_producer = nil
      else
        @producers_mutex.synchronize {
          shutdown_threads = @producers.map { |key, producer|
            th = Thread.new {
              close_producer(producer)
            }
            th.abort_on_exception = true
            th
          }
          shutdown_threads.each { |th| th.join }
          @producers = {}
        }
      end
    end

    def close_producer(producer)
      unless producer.close(10)
        log.warn("Queue is forcefully closed after 10 seconds wait")
      end
    end

    def get_producer
      if @share_producer
        @shared_producer
      else
        @producers_mutex.synchronize {
          producer = @producers[Thread.current.object_id]
          unless producer
            producer = @rdkafka.producer
            @producers[Thread.current.object_id] = producer
          end
          producer
        }
      end
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
      topic = (chunk.metadata.variables && chunk.metadata.variables[@topic_key_sym]) || @default_topic || tag

      handlers = []
      record_buf = nil
      record_buf_bytes = nil

      headers = @headers.clone

      begin
        producer = get_producer
        chunk.msgpack_each { |time, record|
          begin
            record = inject_values_to_record(tag, time, record)
            record.delete(@topic_key) if @exclude_topic_key
            partition = (@exclude_partition ? record.delete(@partition_key) : record[@partition_key]) || @default_partition
            message_key = (@exclude_message_key ? record.delete(@message_key_key) : record[@message_key_key]) || @default_message_key

            @headers_from_record_accessors.each do |key, header_accessor|
              headers[key] = header_accessor.call(record)
            end

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

          handler = enqueue_with_retry(producer, topic, record_buf, message_key, partition, headers)
          if @rdkafka_delivery_handle_poll_timeout != 0
            handlers << handler
          end
        }
        handlers.each { |handler|
          handler.wait(max_wait_timeout: @rdkafka_delivery_handle_poll_timeout)
        }
      end
    rescue Exception => e
      log.warn "Send exception occurred: #{e} at #{e.backtrace.first}"
      # Raise exception to retry sendind messages
      raise e
    end

    def enqueue_with_retry(producer, topic, record_buf, message_key, partition, headers)
      attempt = 0
      loop do
        begin
          return producer.produce(topic: topic, payload: record_buf, key: message_key, partition: partition, headers: headers)
        rescue Exception => e
          if e.respond_to?(:code) && e.code == :queue_full
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
