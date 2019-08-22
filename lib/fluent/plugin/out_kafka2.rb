require 'fluent/plugin/output'
require 'fluent/plugin/kafka_plugin_util'

require 'kafka'
require 'fluent/plugin/kafka_producer_ext'

module Fluent::Plugin
  class Fluent::Kafka2Output < Output
    Fluent::Plugin.register_output('kafka2', self)

    helpers :inject, :formatter, :event_emitter, :record_accessor

    config_param :brokers, :array, :value_type => :string, :default => ['localhost:9092'],
                 :desc => <<-DESC
Set brokers directly:
<broker1_host>:<broker1_port>,<broker2_host>:<broker2_port>,..
DESC
    config_param :topic_key, :string, :default => 'topic', :desc => "Field for kafka topic"
    config_param :default_topic, :string, :default => nil,
                 :desc => "Default output topic when record doesn't have topic field"
    config_param :message_key_key, :string, :default => 'message_key', :desc => "Field for kafka message key"
    config_param :default_message_key, :string, :default => nil
    config_param :partition_key_key, :string, :default => 'partition_key', :desc => "Field for kafka partition key"
    config_param :default_partition_key, :string, :default => nil
    config_param :partition_key, :string, :default => 'partition', :desc => "Field for kafka partition"
    config_param :default_partition, :integer, :default => nil
    config_param :use_default_for_unknown_topic, :bool, :default => false, :desc => "If true, default_topic is used when topic not found"
    config_param :client_id, :string, :default => 'fluentd'
    config_param :idempotent, :bool, :default => false, :desc => 'Enable idempotent producer'
    config_param :sasl_over_ssl, :bool, :default => true,
               :desc => <<-DESC
Set to false to prevent SSL strict mode when using SASL authentication
DESC
    config_param :exclude_partition_key, :bool, :default => false,
                 :desc => 'Set true to remove partition key from data'
    config_param :exclude_partition, :bool, :default => false,
                 :desc => 'Set true to remove partition from data'
    config_param :exclude_message_key, :bool, :default => false,
                 :desc => 'Set true to remove partition key from data'
    config_param :exclude_topic_key, :bool, :default => false,
                 :desc => 'Set true to remove topic name key from data'
    config_param :use_event_time, :bool, :default => false, :desc => 'Use fluentd event time for kafka create_time'
    config_param :headers, :hash, default: {}, symbolize_keys: true, value_type: :string,
                 :desc => 'Kafka message headers'
    config_param :headers_from_record, :hash, default: {}, symbolize_keys: true, value_type: :string,
                 :desc => 'Kafka message headers where the header value is a jsonpath to a record value'

    config_param :get_kafka_client_log, :bool, :default => false

    config_param :ignore_exceptions, :array, :default => [], value_type: :string, :desc => "Ignorable exception list"
    config_param :exception_backup, :bool, :default => true, :desc => "Chunk backup flag when ignore exception occured"

    config_param :connect_timeout, :integer, :default => nil,
                 :desc => "The timeout setting for connecting to brokers"
    config_param :socket_timeout, :integer, :default => nil,
                 :desc => "The timeout setting for socket connection"

    # ruby-kafka producer options
    config_param :max_send_retries, :integer, :default => 2,
                 :desc => "Number of times to retry sending of messages to a leader."
    config_param :required_acks, :integer, :default => -1,
                 :desc => "The number of acks required per request."
    config_param :ack_timeout, :time, :default => nil,
                 :desc => "How long the producer waits for acks."
    config_param :compression_codec, :string, :default => nil,
                 :desc => <<-DESC
The codec the producer uses to compress messages.
Supported codecs depends on ruby-kafka: https://github.com/zendesk/ruby-kafka#compression
DESC
    config_param :max_send_limit_bytes, :size, :default => nil
    config_param :active_support_notification_regex, :string, :default => nil,
                 :desc => <<-DESC
Add a regular expression to capture ActiveSupport notifications from the Kafka client
requires activesupport gem - records will be generated under fluent_kafka_stats.**
DESC

    config_section :buffer do
      config_set_default :chunk_keys, ["topic"]
    end
    config_section :format do
      config_set_default :@type, 'json'
    end

    include Fluent::KafkaPluginUtil::SSLSettings
    include Fluent::KafkaPluginUtil::SaslSettings

    def initialize
      super

      @kafka = nil
    end

    def refresh_client(raise_error = true)
      begin
        logger = @get_kafka_client_log ? log : nil
        if @scram_mechanism != nil && @username != nil && @password != nil
          @kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id, logger: logger, connect_timeout: @connect_timeout, socket_timeout: @socket_timeout, ssl_ca_cert: read_ssl_file(@ssl_ca_cert),
                             ssl_client_cert: read_ssl_file(@ssl_client_cert), ssl_client_cert_key: read_ssl_file(@ssl_client_cert_key), ssl_client_cert_chain: read_ssl_file(@ssl_client_cert_chain),
                             ssl_ca_certs_from_system: @ssl_ca_certs_from_system, sasl_scram_username: @username, sasl_scram_password: @password,
                             sasl_scram_mechanism: @scram_mechanism, sasl_over_ssl: @sasl_over_ssl, ssl_verify_hostname: @ssl_verify_hostname)
        elsif @username != nil && @password != nil
          @kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id, logger: logger, connect_timeout: @connect_timeout, socket_timeout: @socket_timeout, ssl_ca_cert: read_ssl_file(@ssl_ca_cert),
                             ssl_client_cert: read_ssl_file(@ssl_client_cert), ssl_client_cert_key: read_ssl_file(@ssl_client_cert_key), ssl_client_cert_chain: read_ssl_file(@ssl_client_cert_chain),
                             ssl_ca_certs_from_system: @ssl_ca_certs_from_system, sasl_plain_username: @username, sasl_plain_password: @password, sasl_over_ssl: @sasl_over_ssl,
                             ssl_verify_hostname: @ssl_verify_hostname)
        else
          @kafka = Kafka.new(seed_brokers: @seed_brokers, client_id: @client_id, logger: logger, connect_timeout: @connect_timeout, socket_timeout: @socket_timeout, ssl_ca_cert: read_ssl_file(@ssl_ca_cert),
                             ssl_client_cert: read_ssl_file(@ssl_client_cert), ssl_client_cert_key: read_ssl_file(@ssl_client_cert_key), ssl_client_cert_chain: read_ssl_file(@ssl_client_cert_chain),
                             ssl_ca_certs_from_system: @ssl_ca_certs_from_system, sasl_gssapi_principal: @principal, sasl_gssapi_keytab: @keytab, sasl_over_ssl: @sasl_over_ssl,
                             ssl_verify_hostname: @ssl_verify_hostname)
        end
        log.info "initialized kafka producer: #{@client_id}"
      rescue Exception => e
        if raise_error # During startup, error should be reported to engine and stop its phase for safety.
          raise e
        else
          log.error e
        end
      end
    end

    def configure(conf)
      super

      if @brokers.size > 0
        @seed_brokers = @brokers
        log.info "brokers has been set: #{@seed_brokers}"
      else
        raise Fluent::Config, 'No brokers specified. Need one broker at least.'
      end

      formatter_conf = conf.elements('format').first
      unless formatter_conf
        raise Fluent::ConfigError, "<format> section is required."
      end
      unless formatter_conf["@type"]
        raise Fluent::ConfigError, "format/@type is required."
      end
      @formatter_proc = setup_formatter(formatter_conf)

      if @default_topic.nil?
        if @use_default_for_unknown_topic
          raise Fluent::ConfigError, "default_topic must be set when use_default_for_unknown_topic is true"
        end
        if @chunk_keys.include?('topic') && !@chunk_key_tag
          log.warn "Use 'topic' field of event record for topic but no fallback. Recommend to set default_topic or set 'tag' in buffer chunk keys like <buffer topic,tag>"
        end
      else
        if @chunk_key_tag
          log.warn "default_topic is set. Fluentd's event tag is not used for topic"
        end
      end

      @producer_opts = {max_retries: @max_send_retries, required_acks: @required_acks, idempotent: @idempotent}
      @producer_opts[:ack_timeout] = @ack_timeout if @ack_timeout
      @producer_opts[:compression_codec] = @compression_codec.to_sym if @compression_codec
      if @active_support_notification_regex
        require 'active_support/notifications'
        require 'active_support/core_ext/hash/keys'
        ActiveSupport::Notifications.subscribe(Regexp.new(@active_support_notification_regex)) do |*args|
          event = ActiveSupport::Notifications::Event.new(*args)
          message = event.payload.respond_to?(:stringify_keys) ? event.payload.stringify_keys : event.payload
          @router.emit("fluent_kafka_stats.#{event.name}", Time.now.to_i, message)
        end
      end

      @topic_key_sym = @topic_key.to_sym

      @headers_from_record_accessors = {}
      @headers_from_record.each do |key, value|
        @headers_from_record_accessors[key] = record_accessor_create(value)
      end
    end

    def multi_workers_ready?
      true
    end

    def start
      super
      refresh_client
    end

    def close
      super
      @kafka.close if @kafka
    end

    def terminate
      super
      @kafka = nil
    end

    def setup_formatter(conf)
      type = conf['@type']
      case type
      when 'json'
        begin
          require 'oj'
          Oj.default_options = Fluent::DEFAULT_OJ_OPTIONS
          Proc.new { |tag, time, record| Oj.dump(record) }
        rescue LoadError
          require 'yajl'
          Proc.new { |tag, time, record| Yajl::Encoder.encode(record) }
        end
      when 'ltsv'
        require 'ltsv'
        Proc.new { |tag, time, record| LTSV.dump(record) }
      else
        @formatter = formatter_create(usage: 'kafka-plugin', conf: conf)
        @formatter.method(:format)
      end
    end

    # TODO: optimize write performance
    def write(chunk)
      tag = chunk.metadata.tag
      topic =  (chunk.metadata.variables && chunk.metadata.variables[@topic_key_sym]) || @default_topic || tag

      messages = 0
      record_buf = nil

      base_headers = @headers
      mutate_headers = !@headers_from_record_accessors.empty?

      begin
        producer = @kafka.topic_producer(topic, @producer_opts)
        chunk.msgpack_each { |time, record|
          begin
            record = inject_values_to_record(tag, time, record)
            record.delete(@topic_key) if @exclude_topic_key
            partition_key = (@exclude_partition_key ? record.delete(@partition_key_key) : record[@partition_key_key]) || @default_partition_key
            partition = (@exclude_partition ? record.delete(@partition_key) : record[@partition_key]) || @default_partition
            message_key = (@exclude_message_key ? record.delete(@message_key_key) : record[@message_key_key]) || @default_message_key

            if mutate_headers
              headers = base_headers.clone
              @headers_from_record_accessors.each do |key, header_accessor|
                headers[key] = header_accessor.call(record)
              end
            else
              headers = base_headers
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

          log.trace { "message will send to #{topic} with partition_key: #{partition_key}, partition: #{partition}, message_key: #{message_key} and value: #{record_buf}." }
          messages += 1

          producer.produce(record_buf, key: message_key, partition_key: partition_key, partition: partition, headers: headers,
                           create_time: @use_event_time ? Time.at(time) : Time.now)
        }

        if messages > 0
          log.debug { "#{messages} messages send." }
          producer.deliver_messages
        end
      rescue Kafka::UnknownTopicOrPartition
        if @use_default_for_unknown_topic && topic != @default_topic
          producer.shutdown if producer
          log.warn "'#{topic}' topic not found. Retry with '#{default_topic}' topic"
          topic = @default_topic
          retry
        end
        raise
      end
    rescue Exception => e
      ignore = @ignore_exceptions.include?(e.class.name)

      log.warn "Send exception occurred: #{e}"
      log.warn "Exception Backtrace : #{e.backtrace.join("\n")}"
      log.warn "Exception ignored in tag : #{tag}" if ignore
      # For safety, refresh client and its producers
      refresh_client(false)
      # raise UnrecoverableError for backup ignored exception chunk
      raise Fluent::UnrecoverableError if ignore && exception_backup
      # Raise exception to retry sendind messages
      raise e unless ignore
    ensure
      producer.shutdown if producer
    end
  end
end
