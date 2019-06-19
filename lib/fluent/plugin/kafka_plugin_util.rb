module Fluent
  module KafkaPluginUtil
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
          config_param :ssl_client_cert_chain, :string, :default => nil,
                       :desc => "an extra PEM encoded cert to use with and SSL connection."
          config_param :ssl_ca_certs_from_system, :bool, :default => false,
                       :desc => "this configures the store to look up CA certificates from the system default certificate store on an as needed basis. The location of the store can usually be determined by: OpenSSL::X509::DEFAULT_CERT_FILE."
          config_param :ssl_verify_hostname, :bool, :default => true,
                       :desc => "this configures whether hostname of certificate should be verified or not."
        }
      end

      DummyFormatter = Object.new

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
        return nil if path.nil?

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
          config_param :scram_mechanism, :string, :default => nil,
                       :desc => "if set, use SCRAM authentication with specified mechanism. When unset, default to PLAIN authentication"
        }
      end
    end
  end
end
