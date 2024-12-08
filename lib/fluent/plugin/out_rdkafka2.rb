require 'thread'
require 'logger'
require 'fluent/plugin/output'
require 'fluent/plugin/kafka_plugin_util'

require 'rdkafka'

begin
  rdkafka_version = Gem::Version::create(Rdkafka::VERSION)
  if rdkafka_version < Gem::Version.create('0.12.0')
    require_relative 'rdkafka_patch/0_11_0'
  elsif rdkafka_version == Gem::Version.create('0.12.0')
    require_relative 'rdkafka_patch/0_12_0'
  elsif rdkafka_version >= Gem::Version.create('0.14.0')
    require_relative 'rdkafka_patch/0_14_0'
  elsif rdkafka_version >= Gem::Version.create('0.16.0')
    require_relative 'rdkafka_patch/0_16_0'
  end
rescue LoadError, NameError
  raise "unable to patch rdkafka."
end

if Gem::Version.create(RUBY_VERSION) >= Gem::Version.create('3.0')
  require 'aws-msk-iam-sasl-signer'
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
    config_param :topic, :string, :default => nil, :desc => "kafka topic. Placeholders are supported"
    config_param :topic_key, :string, :default => 'topic', :desc => "Field for kafka topic"
    config_param :default_topic, :string, :default => nil,
                 :desc => "Default output topic when record doesn't have topic field"
    config_param :use_default_for_unknown_topic, :bool, :default => false, :desc => "If true, default_topic is used when topic not found"
    config_param :use_default_for_unknown_partition_error, :bool, :default => false, :desc => "If true, default_topic is used when received unknown_partition error"
    config_param :message_key_key, :string, :default => 'message_key', :desc => "Field for kafka message key"
    config_param :default_message_key, :string, :default => nil
    config_param :partition_key, :string, :default => 'partition', :desc => "Field for kafka partition"
    config_param :default_partition, :integer, :default => nil
    config_param :idempotent, :bool, :default => false, :desc => 'Enable idempotent producer'
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
    config_param :exclude_fields, :array, :default => [], value_type: :string,
                 :desc => 'Fields to remove from data where the value is a jsonpath to a record value'
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
    config_param :record_key, :string, :default => nil,
                 :desc => <<-DESC
A jsonpath to a record value pointing to the field which will be passed to the formatter and sent as the Kafka message payload.
If defined, only this field in the record will be sent to Kafka as the message payload.
DESC
    config_param :use_event_time, :bool, :default => false, :desc => 'Use fluentd event time for rdkafka timestamp'
    config_param :max_send_limit_bytes, :size, :default => nil
    config_param :discard_kafka_delivery_failed, :bool, :default => false
    config_param :discard_kafka_delivery_failed_regex, :regexp, :default => nil
    config_param :rdkafka_buffering_max_ms, :integer, :default => nil, :desc => 'Used for queue.buffering.max.ms'
    config_param :rdkafka_buffering_max_messages, :integer, :default => nil, :desc => 'Used for queue.buffering.max.messages'
    config_param :rdkafka_message_max_bytes, :integer, :default => nil, :desc => 'Used for message.max.bytes'
    config_param :rdkafka_message_max_num, :integer, :default => nil, :desc => 'Used for batch.num.messages'
    config_param :rdkafka_delivery_handle_poll_timeout, :integer, :default => 30, :desc => 'Timeout for polling message wait'
    config_param :rdkafka_options, :hash, :default => {}, :desc => 'Set any rdkafka configuration. See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md'
    config_param :share_producer, :bool, :default => false, :desc => 'share kafka producer between flush threads'

    config_param :max_enqueue_retries, :integer, :default => 3
    config_param :enqueue_retry_backoff, :integer, :default => 3
    config_param :max_enqueue_bytes_per_second, :size, :default => nil, :desc => 'The maximum number of enqueueing bytes per second'

    config_param :service_name, :string, :default => nil, :desc => 'Used for sasl.kerberos.service.name'
    config_param :unrecoverable_error_codes, :array, :default => ["topic_authorization_failed", "msg_size_too_large"],
                 :desc => 'Handle some of the error codes should be unrecoverable if specified'

    config_param :aws_msk_region, :string, :default => nil, :desc => 'AWS region for MSK'

    config_section :buffer do
      config_set_default :chunk_keys, ["topic"]
    end
    config_section :format do
      config_set_default :@type, 'json'
      config_set_default :add_newline, false
    end

    include Fluent::KafkaPluginUtil::SSLSettings
    include Fluent::KafkaPluginUtil::SaslSettings

    class EnqueueRate
      class LimitExceeded < StandardError
        attr_reader :next_retry_clock
        def initialize(next_retry_clock)
          @next_retry_clock = next_retry_clock
        end
      end

      def initialize(limit_bytes_per_second)
        @mutex = Mutex.new
        @start_clock = Fluent::Clock.now
        @bytes_per_second = 0
        @limit_bytes_per_second = limit_bytes_per_second
        @commits = {}
      end

      def raise_if_limit_exceeded(bytes_to_enqueue)
        return if @limit_bytes_per_second.nil?

        @mutex.synchronize do
          @commits[Thread.current] = {
            clock: Fluent::Clock.now,
            bytesize: bytes_to_enqueue,
          }

          @bytes_per_second += @commits[Thread.current][:bytesize]
          duration = @commits[Thread.current][:clock] - @start_clock

          if duration < 1.0
            if @bytes_per_second > @limit_bytes_per_second
              raise LimitExceeded.new(@start_clock + 1.0)
            end
          else
            @start_clock = @commits[Thread.current][:clock]
            @bytes_per_second = @commits[Thread.current][:bytesize]
          end
        end
      end

      def revert
        return if @limit_bytes_per_second.nil?

        @mutex.synchronize do
          return unless @commits[Thread.current]
          return unless @commits[Thread.current][:clock]
          if @commits[Thread.current][:clock] >= @start_clock
            @bytes_per_second -= @commits[Thread.current][:bytesize]
          end
          @commits[Thread.current] = nil
        end
      end
    end

    def initialize
      super

      @producers = nil
      @producers_mutex = nil
      @shared_producer = nil
      @enqueue_rate = nil
      @writing_threads_mutex = Mutex.new
      @writing_threads = Set.new
    end

    def configure(conf)
      super
      log.instance_eval {
        def add(level, message = nil)
          if message.nil?
            if block_given?
              message = yield
            else
              return
            end
          end

          # Follow rdkakfa's log level. See also rdkafka-ruby's bindings.rb: https://github.com/appsignal/rdkafka-ruby/blob/e5c7261e3f2637554a5c12b924be297d7dca1328/lib/rdkafka/bindings.rb#L117
          case level
          when Logger::FATAL
            self.fatal(message)
          when Logger::ERROR
            self.error(message)
          when Logger::WARN
            self.warn(message)
          when Logger::INFO
            self.info(message)
          when Logger::DEBUG
            self.debug(message)
          else
            self.trace(message)
          end
        end
      }
      Rdkafka::Config.logger = log
      config = build_config
      @rdkafka = Rdkafka::Config.new(config)


      if config[:"security.protocol"] == "sasl_ssl" && config[:"sasl.mechanisms"] == "OAUTHBEARER"
        Rdkafka::Config.oauthbearer_token_refresh_callback = method(:refresh_token)
      end

      if @default_topic.nil?
        if @use_default_for_unknown_topic || @use_default_for_unknown_partition_error
          raise Fluent::ConfigError, "default_topic must be set when use_default_for_unknown_topic or use_default_for_unknown_partition_error is true"
        end
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

      @exclude_field_accessors = @exclude_fields.map do |field|
        record_accessor_create(field)
      end

      @enqueue_rate = EnqueueRate.new(@max_enqueue_bytes_per_second) unless @max_enqueue_bytes_per_second.nil?

      @record_field_accessor = nil
      @record_field_accessor = record_accessor_create(@record_key) unless @record_key.nil?
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
      config[:"sasl.username"] = @username if @username
      config[:"sasl.password"] = @password if @password
      config[:"enable.idempotence"] = @idempotent if @idempotent

      # sasl.mechnisms and security.protocol are set as rdkafka_options
      @rdkafka_options.each { |k, v|
        config[k.to_sym] = v
      }

      config
    end

    def refresh_token(_config, _client_name)
      log.info("+--- Refreshing token")
      client = get_producer
      # This will happen once upon initialization and is expected to fail, as the producer isnt set yet
      # We will set the token manually after creation and after that this refresh method will work
      unless client
        log.info("Could not get shared client handle, unable to set/refresh token (this is expected one time on startup)")
        return
      end
      signer = AwsMskIamSaslSigner::MSKTokenProvider.new(region: @aws_msk_region)
      token = signer.generate_auth_token

      if token
        client.oauthbearer_set_token(
          token: token.token,
          lifetime_ms: token.expiration_time_ms,
          principal_name: "kafka-cluster"
        )
      else
        client.oauthbearer_set_token_failure(
          "Failed to generate token."
        )
      end
    end

    def start
      if @share_producer
        @shared_producer = @rdkafka.producer
        log.info("Created shared producer")
        if @aws_msk_region
          refresh_token(nil, nil)
          log.info("Set initial token for shared producer")
        end
      else
        @producers = {}
        @producers_mutex = Mutex.new
      end

      super
    end

    def multi_workers_ready?
      true
    end

    def wait_writing_threads
      done = false
      until done do
        @writing_threads_mutex.synchronize do
          done = true if @writing_threads.empty?
        end
        sleep(1) unless done
      end
    end

    def shutdown
      super
      wait_writing_threads
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
      @writing_threads_mutex.synchronize { @writing_threads.add(Thread.current) }
      tag = chunk.metadata.tag
      topic = if @topic
                extract_placeholders(@topic, chunk)
              else
                (chunk.metadata.variables && chunk.metadata.variables[@topic_key_sym]) || @default_topic || tag
              end

      handlers = []

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

            unless @exclude_fields.empty?
              @exclude_field_accessors.each do |exclude_field_acessor|
                exclude_field_acessor.delete(record)
              end
            end

            record = @record_field_accessor.call(record) unless @record_field_accessor.nil?
            record_buf = @formatter_proc.call(tag, time, record)
            record_buf_bytes = record_buf.bytesize
            if @max_send_limit_bytes && record_buf_bytes > @max_send_limit_bytes
              log.warn "record size exceeds max_send_limit_bytes. Skip event:", :time => time, :record_size => record_buf_bytes
              log.debug "Skipped event:", :record => record
              next
            end
          rescue StandardError => e
            log.warn "unexpected error during format record. Skip broken event:", :error => e.to_s, :error_class => e.class.to_s, :time => time, :record => record
            next
          end

          handler = enqueue_with_retry(producer, topic, record_buf, message_key, partition, headers, time)
          if @rdkafka_delivery_handle_poll_timeout != 0
            handlers << handler
          end
        }
        handlers.each { |handler|
          handler.wait(max_wait_timeout: @rdkafka_delivery_handle_poll_timeout)
        }
      end
    rescue Exception => e
      if @discard_kafka_delivery_failed
        log.warn "Delivery failed. Discard events:", :error => e.to_s, :error_class => e.class.to_s, :tag => tag
      else
	if @discard_kafka_delivery_failed_regex != nil && @discard_kafka_delivery_failed_regex.match?(e.to_s)
          log.warn "Delivery failed and matched regexp pattern #{@discard_kafka_delivery_failed_regex}. Discard events:", :error => e.to_s, :error_class => e.class.to_s, :tag => tag
	else
	  log.warn "Send exception occurred: #{e} at #{e.backtrace.first}"
          # Raise exception to retry sendind messages
          raise e
	end
      end
    ensure
      @writing_threads_mutex.synchronize { @writing_threads.delete(Thread.current) }
    end

    def enqueue_with_retry(producer, topic, record_buf, message_key, partition, headers, time)
      attempt = 0
      actual_topic = topic

      loop do
        begin
          @enqueue_rate.raise_if_limit_exceeded(record_buf.bytesize) if @enqueue_rate
          return producer.produce(topic: actual_topic, payload: record_buf, key: message_key, partition: partition, headers: headers, timestamp: @use_event_time ? Time.at(time) : nil)
        rescue EnqueueRate::LimitExceeded => e
          @enqueue_rate.revert if @enqueue_rate
          duration = e.next_retry_clock - Fluent::Clock.now
          sleep(duration) if duration > 0.0
        rescue Exception => e
          @enqueue_rate.revert if @enqueue_rate

          if !e.respond_to?(:code)
            raise e
          end

          case e.code
          when :queue_full
            if attempt <= @max_enqueue_retries
              log.warn "Failed to enqueue message; attempting retry #{attempt} of #{@max_enqueue_retries} after #{@enqueue_retry_backoff}s"
              sleep @enqueue_retry_backoff
              attempt += 1
            else
              raise "Failed to enqueue message although tried retry #{@max_enqueue_retries} times"
            end
          # https://github.com/confluentinc/librdkafka/blob/c282ba2423b2694052393c8edb0399a5ef471b3f/src/rdkafka.h#LL309C9-L309C41
          # RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC
          when :unknown_topic
            if @use_default_for_unknown_topic && actual_topic != @default_topic
              log.debug "'#{actual_topic}' topic not found. Retry with '#{@default_topic}' topic"
              actual_topic = @default_topic
              retry
            end
            raise e
          # https://github.com/confluentinc/librdkafka/blob/c282ba2423b2694052393c8edb0399a5ef471b3f/src/rdkafka.h#L305
          # RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION
          when :unknown_partition
            if @use_default_for_unknown_partition_error && actual_topic != @default_topic
              log.debug "failed writing to topic '#{actual_topic}' with error '#{e.to_s}'. Writing message to topic '#{@default_topic}'"
              actual_topic = @default_topic
              retry
            end

            raise e
          else
            if unrecoverable_error_codes.include?(e.code.to_s)
              # some of the errors should be handled as an unrecoverable error
              raise Fluent::UnrecoverableError, "Rejected due to #{e}"
            else
              raise e
            end
          end
        end
      end
    end
  end
end
