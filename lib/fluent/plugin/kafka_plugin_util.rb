module Fluent
  module KafkaPluginUtil
    module PartitionSettings
      # -1 is the unassigned partition; the Kafka protocol takes the index as int32
      PARTITION_RANGE = -1..(2**31 - 1)

      def coerce_partition(partition)
        partition = case partition
                    when Integer
                      partition
                    when String
                      Integer(partition, 10)
                    else
                      raise TypeError, "partition must be an Integer or a decimal String, got #{partition.class}"
                    end
        raise RangeError, "partition #{partition} is out of range" unless PARTITION_RANGE.cover?(partition)

        partition
      end
    end

    module AwsIamSettings
      def self.included(klass)
        klass.instance_eval do
          config_param :sasl_aws_msk_iam_access_key_id, :string, :default => nil, secret: true,
                       desc: "AWS access key Id for IAM authentication to MSK."
          config_param :sasl_aws_msk_iam_secret_key_id, :string, :default => nil, secret: true,
                       desc: "AWS access key secret for IAM authentication to MSK."
          config_param :sasl_aws_msk_iam_aws_region, :string, :default => nil,
                       desc: "AWS region for IAM authentication to MSK."
        end
      end
    end

    module SSLSettings
      def self.included(klass)
        klass.instance_eval {
          # https://github.com/zendesk/ruby-kafka#encryption-and-authentication-using-ssl
          config_param :ssl_ca_cert, :array, :value_type => :string, :default => nil,
                       :desc => "a PEM encoded CA cert to use with and SSL connection."
          config_param :ssl_client_cert, :string, :default => nil,
                       :desc => "a PEM encoded client cert to use with and SSL connection. Must be used in combination with ssl_client_cert_key."
          config_param :ssl_client_cert_key, :string, :default => nil,
                       :desc => "a PEM encoded client cert key to use with and SSL connection. Must be used in combination with ssl_client_cert."
          config_param :ssl_client_cert_key_password, :string, :default => nil, secret: true,
                       :desc => "a PEM encoded client cert key password to use with SSL connection."
          config_param :ssl_client_cert_chain, :string, :default => nil,
                       :desc => "an extra PEM encoded cert to use with and SSL connection."
          config_param :ssl_ca_certs_from_system, :bool, :default => false,
                       :desc => "this configures the store to look up CA certificates from the system default certificate store on an as needed basis. The location of the store can usually be determined by: OpenSSL::X509::DEFAULT_CERT_FILE."
          config_param :ssl_verify_hostname, :bool, :default => true,
                       :desc => "this configures whether hostname of certificate should be verified or not."
        }
      end

      DummyFormatter = Object.new

      def configure(conf)
        super

        if @ssl_client_cert && !@ssl_client_cert_key
          raise Fluent::ConfigError, "ssl_client_cert is set but ssl_client_cert_key is not. Please provide both."
        elsif !@ssl_client_cert && @ssl_client_cert_key
          raise Fluent::ConfigError, "ssl_client_cert_key is set but ssl_client_cert is not. Please provide both."
        elsif @ssl_client_cert_chain && !@ssl_client_cert
          raise Fluent::ConfigError, "ssl_client_cert_chain is set but ssl_client_cert is not. Please provide cert, key and chain."
        elsif @ssl_client_cert_key_password && !@ssl_client_cert_key
          raise Fluent::ConfigError, "ssl_client_cert_key_password is set but ssl_client_cert_key is not. Please provide both."
        end
      end

      def start
        super

        # This is bad point here but easy to fix for all kafka plugins
        unless log.respond_to?(:formatter)
          def log.formatter
            Fluent::KafkaPluginUtil::SSLSettings::DummyFormatter
          end
        end
      end

      def read_ssl_file(path)
        return nil if path.nil? || path.respond_to?(:strip) && path.strip.empty?

        if path.is_a?(Array)
          path.map { |fp| File.read(fp) }
        else
          File.read(path)
        end
      end

      def pickup_ssl_endpoint(node)
        ssl_endpoint = node['endpoints'].find {|e| e.start_with?('SSL')}
        raise 'no SSL endpoint found on Zookeeper' unless ssl_endpoint
        return [URI.parse(ssl_endpoint).host, URI.parse(ssl_endpoint).port].join(':')
      end
    end

    module SaslSettings
      def self.included(klass)
        klass.instance_eval {
          config_param :principal, :string, :default => nil,
                       :desc => "a Kerberos principal to use with SASL authentication (GSSAPI)."
          config_param :keytab, :string, :default => nil,
                       :desc => "a filepath to Kerberos keytab. Must be used with principal."
          config_param :username, :string, :default => nil,
                       :desc => "a username when using PLAIN/SCRAM SASL authentication"
          config_param :password, :string, :default => nil, secret: true,
                       :desc => "a password when using PLAIN/SCRAM SASL authentication"
          config_param :scram_mechanism, :enum, :list => [:sha256, :sha512], :default => nil,
                       :desc => "if set, use SCRAM authentication with specified mechanism. When unset, default to PLAIN authentication"
        }
      end

      def configure(conf)
        super

        @scram_mechanism = @scram_mechanism.to_s if @scram_mechanism
      end
    end

    module RdkafkaSecuritySettings
      SCRAM_MECHANISMS = {
        "sha256" => "SCRAM-SHA-256",
        "sha512" => "SCRAM-SHA-512",
      }.freeze

      def self.included(klass)
        klass.instance_eval {
          config_param :service_name, :string, :default => nil, :desc => 'Used for sasl.kerberos.service.name'
          config_param :sasl_over_ssl, :bool, :default => true,
                       :desc => 'When true, SASL authentication requires an SSL connection'
        }
      end

      def rdkafka_security_config
        config = {}

        if (@ssl_ca_cert && @ssl_ca_cert[0]) || @ssl_ca_certs_from_system || @ssl_client_cert || @ssl_client_cert_key
          ssl = true
          config[:"ssl.ca.location"] = @ssl_ca_cert[0] if @ssl_ca_cert && @ssl_ca_cert[0]
          config[:"ssl.certificate.location"] = @ssl_client_cert if @ssl_client_cert
          config[:"ssl.key.location"] = @ssl_client_cert_key if @ssl_client_cert_key
          config[:"ssl.key.password"] = @ssl_client_cert_key_password if @ssl_client_cert_key_password
          config[:"ssl.endpoint.identification.algorithm"] = @ssl_verify_hostname ? "https" : "none"
          config[:"enable.ssl.certificate.verification"] = true
        end

        if @principal
          sasl = true
          config[:"sasl.mechanisms"] = "GSSAPI"
          config[:"sasl.kerberos.principal"] = @principal
          config[:"sasl.kerberos.service.name"] = @service_name if @service_name
          config[:"sasl.kerberos.keytab"] = @keytab if @keytab
        end

        if @username && @password
          sasl = true
          config[:"sasl.mechanisms"] = SCRAM_MECHANISMS.fetch(@scram_mechanism, 'PLAIN')
        elsif @scram_mechanism
          log.warn "scram_mechanism is ignored because username and password are not set"
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

        config[:"sasl.username"] = @username if @username
        config[:"sasl.password"] = @password if @password

        config
      end

      def validate_sasl_over_ssl(config)
        if @sasl_over_ssl && config[:"sasl.password"] && config[:"security.protocol"].to_s.upcase == "SASL_PLAINTEXT"
          raise Fluent::ConfigError, "SASL authentication requires that SSL is configured. Set 'sasl_over_ssl false' to send SASL credentials over a plaintext connection"
        end
      end
    end
  end
end
