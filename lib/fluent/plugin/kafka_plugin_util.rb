module Fluent
  module KafkaPluginUtil
    module SSLSettings
      def self.included(klass)
        klass.instance_eval {
          config_param :ssl_ca_cert, :string, :default => nil,
                       :desc => "a PEM encoded CA cert to use with and SSL connection."
          config_param :ssl_client_cert, :string, :default => nil,
                       :desc => "a PEM encoded client cert to use with and SSL connection. Must be used in combination with ssl_client_cert_key."
          config_param :ssl_client_cert_key, :string, :default => nil,
                       :desc => "a PEM encoded client cert key to use with and SSL connection. Must be used in combination with ssl_client_cert."
        }
      end

      def read_ssl_file(path)
        return nil if path.nil?
        File.read(path)
      end
    end
  end
end
